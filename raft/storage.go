package raft

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Storage 负责节点状态的文件持久化：
//   - state.json    选举硬状态（term / votedFor）
//   - wal.log       追加式日志（JSON Lines）
//   - snapshot.json 快照（含状态机数据与幂等表）
//
// 所有多步写操作先写临时文件再 rename，并显式 Sync，
// 保证恢复时要么看到旧状态、要么看到新状态，绝不留下部分写入的中间态。
type Storage struct {
	dir string
	log func(string, ...any)
}

func openStorage(dir string, logf func(string, ...any)) (*Storage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("创建持久化目录 %s: %w", dir, err)
	}
	return &Storage{dir: dir, log: logf}, nil
}

func (s *Storage) logf(format string, args ...any) {
	if s.log != nil {
		s.log("[storage] "+format, args...)
	}
}

func (s *Storage) path(name string) string { return filepath.Join(s.dir, name) }

// writeFileAtomic 写临时文件 + Sync + rename，保证原子替换。
func (s *Storage) writeFileAtomic(name string, data []byte) error {
	tmp := s.path(name + ".tmp")
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
	if err := os.Rename(tmp, s.path(name)); err != nil {
		return err
	}
	// Sync 目录，确保 rename 本身落盘。
	if d, err := os.Open(s.dir); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

func (s *Storage) loadHardState() (HardState, error) {
	var hs HardState
	data, err := os.ReadFile(s.path("state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return hs, nil
	}
	if err != nil {
		return hs, err
	}
	if err := json.Unmarshal(data, &hs); err != nil {
		return hs, fmt.Errorf("解析 state.json: %w", err)
	}
	s.logf("加载硬状态 term=%d votedFor=%q", hs.CurrentTerm, hs.VotedFor)
	return hs, nil
}

func (s *Storage) saveHardState(hs HardState) error {
	data, err := json.Marshal(hs)
	if err != nil {
		return err
	}
	s.logf("落盘硬状态 term=%d votedFor=%q", hs.CurrentTerm, hs.VotedFor)
	return s.writeFileAtomic("state.json", data)
}

// loadEntries 读取 WAL 中的全部日志条目。
func (s *Storage) loadEntries() ([]LogEntry, error) {
	f, err := os.Open(s.path("wal.log"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var entries []LogEntry
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			var e LogEntry
			if jerr := json.Unmarshal(line, &e); jerr != nil {
				// 最后一行可能是宕机时的半行写入，丢弃并截断后续内容。
				s.logf("WAL 中发现损坏行，忽略其后内容: %v", jerr)
				break
			}
			entries = append(entries, e)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	if len(entries) > 0 {
		s.logf("加载 WAL 共 %d 条, 范围 [%d..%d]", len(entries), entries[0].Index, entries[len(entries)-1].Index)
	}
	return entries, nil
}

// appendEntries 将条目追加到 WAL 并落盘。
func (s *Storage) appendEntries(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	f, err := os.OpenFile(s.path("wal.log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, e := range entries {
		data, err := json.Marshal(e)
		if err != nil {
			f.Close()
			return err
		}
		w.Write(data)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	err = f.Close()
	s.logf("WAL 追加 %d 条, 末位 index=%d term=%d", len(entries), entries[len(entries)-1].Index, entries[len(entries)-1].Term)
	return err
}

// truncateAndAppend 先重写 WAL 为 keep（保留前缀），再追加 entries。
// 整体通过临时文件 + rename 完成，宕机不会留下"截断一半"的日志。
func (s *Storage) truncateAndAppend(keep []LogEntry, entries []LogEntry) error {
	var buf []byte
	enc := func(e LogEntry) {
		data, _ := json.Marshal(e)
		buf = append(buf, data...)
		buf = append(buf, '\n')
	}
	for _, e := range keep {
		enc(e)
	}
	for _, e := range entries {
		enc(e)
	}
	if err := s.writeFileAtomic("wal.log", buf); err != nil {
		return err
	}
	last := uint64(0)
	if n := len(keep) + len(entries); n > 0 {
		all := append(append([]LogEntry{}, keep...), entries...)
		last = all[n-1].Index
	}
	s.logf("WAL 截断重写: 保留 %d 条, 追加 %d 条, 末位 index=%d", len(keep), len(entries), last)
	return nil
}

func (s *Storage) loadSnapshot() (*SnapshotMeta, error) {
	data, err := os.ReadFile(s.path("snapshot.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var meta SnapshotMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("解析 snapshot.json: %w", err)
	}
	s.logf("加载快照 lastIncludedIndex=%d lastIncludedTerm=%d 状态机键数=%d",
		meta.LastIncludedIndex, meta.LastIncludedTerm, len(meta.Data))
	return &meta, nil
}

// saveSnapshot 原子地写入快照，并将 WAL 重写为 remaining（快照之后的增量日志）。
// 先写快照再重写 WAL：若中途宕机，恢复时快照与 WAL 至少有一个是完整的旧版本，
// 不会出现"快照已生效但 WAL 已截断"的部分应用状态。
func (s *Storage) saveSnapshot(meta *SnapshotMeta, remaining []LogEntry) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := s.writeFileAtomic("snapshot.json", data); err != nil {
		return err
	}
	s.logf("落盘快照 lastIncludedIndex=%d lastIncludedTerm=%d", meta.LastIncludedIndex, meta.LastIncludedTerm)
	var buf []byte
	for _, e := range remaining {
		line, _ := json.Marshal(e)
		buf = append(buf, line...)
		buf = append(buf, '\n')
	}
	if err := s.writeFileAtomic("wal.log", buf); err != nil {
		return err
	}
	s.logf("WAL 压缩: 快照后剩余 %d 条", len(remaining))
	return nil
}
