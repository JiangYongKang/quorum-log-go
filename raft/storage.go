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
