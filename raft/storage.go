package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// hardState 是需要在每次投票/写日志前落盘的持久化状态。
type hardState struct {
	Term     int
	VotedFor string
}

// CrashPoint 标识一个可注入崩溃的落盘步骤边界（测试用）。
type CrashPoint struct {
	Dir  string // 节点数据目录
	Step string // 步骤名，如 tmp-written / snapshot-written / log-compacting
}

// crashHook 在关键落盘步骤之间被调用；返回 true 表示“此刻进程被强杀”，
// 当前存储操作立即以 errSimulatedCrash 失败，磁盘上保留未完成的中途状态。
// 仅测试注入使用，生产环境为 nil。
var crashHook func(CrashPoint) bool

// errSimulatedCrash 表示在注入点模拟的进程强杀：操作未完成，磁盘保留中途状态。
var errSimulatedCrash = errors.New("simulated crash at disk step")

// checkCrash 在当前步骤边界询问是否注入强杀。
func checkCrash(dir, step string) error {
	if crashHook != nil && crashHook(CrashPoint{Dir: dir, Step: step}) {
		return errSimulatedCrash
	}
	return nil
}

const (
	hardStateFile = "hardstate.json"
	logDirName    = "log"
	snapshotFile  = "snapshot.json"
)

// Storage 负责节点持久化：硬状态、日志条目与快照。
// 硬状态与快照通过“写临时文件 + rename”原子替换；
// 日志每条记录一个文件（带 CRC），追加前 fsync，保证崩溃后可恢复到一致前缀。
type Storage struct {
	mu  sync.Mutex
	dir string
}

// OpenStorage 打开（或创建）目录下的持久化存储。
func OpenStorage(dir string) (*Storage, error) {
	if err := os.MkdirAll(filepath.Join(dir, logDirName), 0o755); err != nil {
		return nil, err
	}
	return &Storage{dir: dir}, nil
}

func writeFileAtomic(dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// 注入点：临时文件已落盘、尚未 rename —— 此刻强杀会留下 .tmp 残留。
	if err := checkCrash(dir, "tmp-written"); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, name))
}

// SaveHardState 原子写入任期与投票记录。
func (s *Storage) SaveHardState(h hardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	return writeFileAtomic(s.dir, hardStateFile, data)
}

// LoadHardState 读取硬状态；首次启动返回零值。
func (s *Storage) LoadHardState() (hardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(filepath.Join(s.dir, hardStateFile))
	if errors.Is(err, os.ErrNotExist) {
		return hardState{}, nil
	}
	if err != nil {
		return hardState{}, err
	}
	var h hardState
	if err := json.Unmarshal(data, &h); err != nil {
		return hardState{}, fmt.Errorf("corrupt hardstate: %w", err)
	}
	return h, nil
}

// 日志条目文件名：log/00000000000000000001.entry，内容为 crc32(4B) + json。
func logEntryPath(dir string, index int) string {
	return filepath.Join(dir, logDirName, fmt.Sprintf("%020d.entry", index))
}

// AppendLog 追加日志条目（逐条落盘并 fsync）。
func (s *Storage) AppendLog(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		payload, err := json.Marshal(e)
		if err != nil {
			return err
		}
		buf := make([]byte, 4+len(payload))
		binary.BigEndian.PutUint32(buf, crc32.ChecksumIEEE(payload))
		copy(buf[4:], payload)
		if err := writeFileAtomic(s.dir, filepath.Join(logDirName, fmt.Sprintf("%020d.entry", e.Index)), buf); err != nil {
			return err
		}
		// 注入点：批量追加的中途 —— 此刻强杀会在磁盘上留下部分批次。
		if err := checkCrash(s.dir, "log-appended"); err != nil {
			return err
		}
	}
	return nil
}

// LoadLog 读取全部日志条目（按下标排序，校验 CRC，损坏即报错）。
func (s *Storage) LoadLog() ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ents, err := os.ReadDir(filepath.Join(s.dir, logDirName))
	if err != nil {
		return nil, err
	}
	names := []string{}
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	out := []LogEntry{}
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(s.dir, logDirName, name))
		if err != nil {
			return nil, err
		}
		if len(raw) < 4 {
			return nil, fmt.Errorf("corrupt log file %s: too short", name)
		}
		payload := raw[4:]
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(raw[:4]) {
			return nil, fmt.Errorf("corrupt log file %s: crc mismatch", name)
		}
		var e LogEntry
		if err := json.Unmarshal(payload, &e); err != nil {
			return nil, fmt.Errorf("corrupt log file %s: %w", name, err)
		}
		out = append(out, e)
	}
	return out, nil
}

// TruncateSuffix 删除从下标 index 开始（含）的日志文件。
func (s *Storage) TruncateSuffix(index int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ents, err := os.ReadDir(filepath.Join(s.dir, logDirName))
	if err != nil {
		return err
	}
	for _, e := range ents {
		var idx int
		if _, err := fmt.Sscanf(e.Name(), "%d.entry", &idx); err != nil {
			continue
		}
		if idx >= index {
			if err := os.Remove(filepath.Join(s.dir, logDirName, e.Name())); err != nil {
				return err
			}
			// 注入点：后缀截断的中途 —— 此刻强杀会留下部分未删除的文件。
			if err := checkCrash(s.dir, "log-truncating"); err != nil {
				return err
			}
		}
	}
	return nil
}

type snapshotMeta struct {
	LastIncludedIndex int             `json:"lastIncludedIndex"`
	LastIncludedTerm  int             `json:"lastIncludedTerm"`
	Data              json.RawMessage `json:"data"`
}

// SaveSnapshot 原子写入快照，并删除已包含的日志前缀文件。
func (s *Storage) SaveSnapshot(lastIndex, lastTerm int, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta := snapshotMeta{LastIncludedIndex: lastIndex, LastIncludedTerm: lastTerm, Data: data}
	buf, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(s.dir, snapshotFile, buf); err != nil {
		return err
	}
	// 注入点：快照已原子落盘、日志压缩尚未开始 —— 此刻强杀会留下过期日志文件。
	if err := checkCrash(s.dir, "snapshot-written"); err != nil {
		return err
	}
	// 压缩：删除 <= lastIndex 的日志文件
	ents, err := os.ReadDir(filepath.Join(s.dir, logDirName))
	if err != nil {
		return err
	}
	for _, e := range ents {
		var idx int
		if _, err := fmt.Sscanf(e.Name(), "%d.entry", &idx); err != nil {
			continue
		}
		if idx <= lastIndex {
			os.Remove(filepath.Join(s.dir, logDirName, e.Name()))
			// 注入点：日志压缩的中途 —— 此刻强杀会留下部分过期日志。
			if err := checkCrash(s.dir, "log-compacting"); err != nil {
				return err
			}
		}
	}
	return nil
}

// LoadSnapshot 读取快照；无快照时 lastIndex 为 0。
func (s *Storage) LoadSnapshot() (lastIndex, lastTerm int, data []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := os.ReadFile(filepath.Join(s.dir, snapshotFile))
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil, nil
	}
	if err != nil {
		return 0, 0, nil, err
	}
	var meta snapshotMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return 0, 0, nil, fmt.Errorf("corrupt snapshot: %w", err)
	}
	return meta.LastIncludedIndex, meta.LastIncludedTerm, meta.Data, nil
}

// LogRecovery 描述磁盘日志恢复的处理结果（供恢复报告使用）。
type LogRecovery struct {
	TruncatedFrom int      // 首个被截断的日志下标（0 表示无截断）
	Removed       []string // 被清理的残留/损坏/过期文件名（相对数据目录）
}

// RecoverLog 在重启时把磁盘日志恢复为一致前缀，并清理上一轮中断的残留：
//  1. 清理中断残留的临时文件（*.tmp，原子替换未完成）；
//  2. 删除已被快照包含（<= snapIndex）的过期日志文件（快照压缩中途强杀的残留）；
//  3. 依次校验余下日志的下标连续性（必须从 snapIndex+1 开始连续）与 CRC，
//     首个损坏或不连续处起的后缀整体截断删除。
//
// 该过程幂等：无论此前被强杀多少次、中断在哪一步，多次执行结果一致；
// 只删除确属残留或损坏的数据，不会误删仍有效的日志。
func (s *Storage) RecoverLog(snapIndex int) ([]LogEntry, LogRecovery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := LogRecovery{}

	// 1) 清理临时文件残留（数据目录与日志目录）
	for _, dir := range []string{s.dir, filepath.Join(s.dir, logDirName)} {
		ents, err := os.ReadDir(dir)
		if err != nil {
			return nil, rec, err
		}
		for _, e := range ents {
			if strings.HasSuffix(e.Name(), ".tmp") {
				os.Remove(filepath.Join(dir, e.Name()))
				rel, _ := filepath.Rel(s.dir, filepath.Join(dir, e.Name()))
				rec.Removed = append(rec.Removed, rel)
			}
		}
	}

	// 2)+3) 读取日志文件，删除过期项，校验连续性与 CRC
	ents, err := os.ReadDir(filepath.Join(s.dir, logDirName))
	if err != nil {
		return nil, rec, err
	}
	names := []string{}
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	out := []LogEntry{}
	expect := snapIndex + 1
	truncated := false
	for _, name := range names {
		var idx int
		if _, err := fmt.Sscanf(name, "%d.entry", &idx); err != nil {
			continue
		}
		path := filepath.Join(s.dir, logDirName, name)
		if truncated || idx <= snapIndex || idx != expect {
			// 过期残留、或从首个空洞/损坏处起的后缀：删除
			if !truncated && idx > snapIndex && idx != expect {
				rec.TruncatedFrom = idx
			}
			if idx > snapIndex {
				truncated = true
				if rec.TruncatedFrom == 0 {
					rec.TruncatedFrom = idx
				}
			}
			os.Remove(path)
			rec.Removed = append(rec.Removed, filepath.Join(logDirName, name))
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, rec, err
		}
		valid := len(raw) >= 4 && crc32.ChecksumIEEE(raw[4:]) == binary.BigEndian.Uint32(raw[:4])
		var e LogEntry
		if valid {
			valid = json.Unmarshal(raw[4:], &e) == nil && e.Index == idx
		}
		if !valid {
			// 损坏文件（撕裂写入）：从该处起截断后缀
			truncated = true
			rec.TruncatedFrom = idx
			os.Remove(path)
			rec.Removed = append(rec.Removed, filepath.Join(logDirName, name))
			continue
		}
		out = append(out, e)
		expect++
	}
	return out, rec, nil
}

// Close 关闭存储。
func (s *Storage) Close() error { return nil }
