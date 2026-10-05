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

	// 故障注入（仅测试）：模拟进程在指定阶段被强杀。
	crashAt string
	crashed bool
}

// ArmCrash 注入崩溃点：到达 stage 阶段后存储进入“已崩溃”状态，
// 当前多步操作中断、后续写操作全部失败，模拟强杀后磁盘保留中间态。
func (s *Storage) ArmCrash(stage string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.crashAt = stage
}

var errCrashed = errors.New("storage crashed (fault injection)")

// checkCrash 在故障注入点检查是否应“当场崩溃”（调用方须持锁）。
func (s *Storage) checkCrash(stage string) error {
	if s.crashed {
		return errCrashed
	}
	if stage != "" && s.crashAt == stage {
		s.crashed = true
		s.crashAt = ""
		return errCrashed
	}
	return nil
}

// Recover 清理上一轮崩溃残留并读出连续有效的日志前缀：
//  1. 删除所有未完成的临时文件（*.tmp）；
//  2. 删除已被快照覆盖（index <= snapIndex）的残留日志；
//  3. 损坏或形成空洞的日志，从第一个异常点起整体截断。
//
// 该操作幂等：无论上一轮在何处中断、重复执行多少次，结果都相同。
func (s *Storage) Recover(snapIndex int) ([]LogEntry, ResidueInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info := ResidueInfo{}

	// 1) 清理临时文件
	for _, dir := range []string{s.dir, filepath.Join(s.dir, logDirName)} {
		ents, err := os.ReadDir(dir)
		if err != nil {
			return nil, info, err
		}
		for _, e := range ents {
			if strings.HasSuffix(e.Name(), ".tmp") {
				os.Remove(filepath.Join(dir, e.Name()))
				rel, _ := filepath.Rel(s.dir, filepath.Join(dir, e.Name()))
				info.TempFiles = append(info.TempFiles, rel)
			}
		}
	}

	// 2) 读取全部日志文件；损坏文件删除并记录为截断点
	byIndex := map[int]LogEntry{}
	ents, err := os.ReadDir(filepath.Join(s.dir, logDirName))
	if err != nil {
		return nil, info, err
	}
	for _, e := range ents {
		name := e.Name()
		if !strings.HasSuffix(name, ".entry") {
			continue
		}
		var idx int
		if _, err := fmt.Sscanf(name, "%d.entry", &idx); err != nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(s.dir, logDirName, name))
		if err != nil {
			return nil, info, err
		}
		entry, ok := decodeLogEntry(raw)
		if !ok {
			// 损坏文件：删除并视为截断点
			os.Remove(filepath.Join(s.dir, logDirName, name))
			info.Truncated = append(info.Truncated, idx)
			continue
		}
		byIndex[idx] = entry
	}

	// 3) 快照已覆盖的残留日志：删除
	for idx := range byIndex {
		if idx <= snapIndex {
			delete(byIndex, idx)
			os.Remove(logEntryPath(s.dir, idx))
			info.StaleLogs = append(info.StaleLogs, idx)
		}
	}

	// 4) 从 snapIndex+1 起走连续前缀；空洞之后的条目整体截断
	valid := []LogEntry{}
	next := snapIndex + 1
	for {
		e, ok := byIndex[next]
		if !ok {
			break
		}
		valid = append(valid, e)
		delete(byIndex, next)
		next++
	}
	for idx := range byIndex {
		os.Remove(logEntryPath(s.dir, idx))
		info.Truncated = append(info.Truncated, idx)
	}

	sort.Ints(info.StaleLogs)
	sort.Ints(info.Truncated)
	sort.Strings(info.TempFiles)
	return valid, info, nil
}

// decodeLogEntry 解码并校验单条日志文件内容（crc32 + json）。
func decodeLogEntry(raw []byte) (LogEntry, bool) {
	if len(raw) < 4 {
		return LogEntry{}, false
	}
	payload := raw[4:]
	if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(raw[:4]) {
		return LogEntry{}, false
	}
	var e LogEntry
	if err := json.Unmarshal(payload, &e); err != nil {
		return LogEntry{}, false
	}
	return e, true
}

// OpenStorage 打开（或创建）目录下的持久化存储。
func OpenStorage(dir string) (*Storage, error) {
	if err := os.MkdirAll(filepath.Join(dir, logDirName), 0o755); err != nil {
		return nil, err
	}
	return &Storage{dir: dir}, nil
}

// writeFileAtomic 写临时文件 + fsync + rename 原子替换（调用方须持锁）。
// 故障注入点 CrashAfterTmpWrite：临时文件落盘后、rename 前“崩溃”，留下 .tmp 残留。
func (s *Storage) writeFileAtomic(dir, name string, data []byte) error {
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
	if err := s.checkCrash(CrashAfterTmpWrite); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, name))
}

// SaveHardState 原子写入任期与投票记录。
func (s *Storage) SaveHardState(h hardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkCrash(""); err != nil {
		return err
	}
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	return s.writeFileAtomic(s.dir, hardStateFile, data)
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
// 故障注入点 CrashMidAppend：批量写到一半“崩溃”，磁盘上只保留已写前缀。
func (s *Storage) AppendLog(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkCrash(""); err != nil {
		return err
	}
	for _, e := range entries {
		payload, err := json.Marshal(e)
		if err != nil {
			return err
		}
		buf := make([]byte, 4+len(payload))
		binary.BigEndian.PutUint32(buf, crc32.ChecksumIEEE(payload))
		copy(buf[4:], payload)
		if err := s.writeFileAtomic(s.dir, filepath.Join(logDirName, fmt.Sprintf("%020d.entry", e.Index)), buf); err != nil {
			return err
		}
		if err := s.checkCrash(CrashMidAppend); err != nil {
			return err
		}
	}
	return nil
}

// TruncateSuffix 删除从下标 index 开始（含）的日志文件。
// 故障注入点 CrashMidTruncate：删到一半“崩溃”，磁盘上留下空洞后缀。
func (s *Storage) TruncateSuffix(index int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkCrash(""); err != nil {
		return err
	}
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
			if err := s.checkCrash(CrashMidTruncate); err != nil {
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
// 故障注入点 CrashSnapshotCompact：快照已替换、日志压缩前“崩溃”，
// 磁盘上留下“新快照 + 旧日志”的残留，由 Recover 清理。
func (s *Storage) SaveSnapshot(lastIndex, lastTerm int, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkCrash(""); err != nil {
		return err
	}
	meta := snapshotMeta{LastIncludedIndex: lastIndex, LastIncludedTerm: lastTerm, Data: data}
	buf, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := s.writeFileAtomic(s.dir, snapshotFile, buf); err != nil {
		return err
	}
	if err := s.checkCrash(CrashSnapshotCompact); err != nil {
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

// Close 关闭存储。
func (s *Storage) Close() error { return nil }
