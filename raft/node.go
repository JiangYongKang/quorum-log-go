package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	ErrNotLeader = errors.New("当前节点不是领导")
	ErrStopped   = errors.New("节点已停止")
)

type proposeReq struct {
	cmd   Command
	reply chan proposeRes
}

type proposeRes struct {
	index uint64
	err   error
}

type readReq struct {
	key   string
	reply chan readRes
}

type readRes struct {
	val string
	ok  bool
}

// Status 节点对外可观测状态。
type Status struct {
	Role        Role
	Term        uint64
	CommitIndex uint64
	LastApplied uint64
	LastIndex   uint64
	LeaderID    string
}

// Node 是一个共识节点，全部状态变更发生在单 goroutine 事件循环内，
// 对外 API 通过 channel 与循环交互，避免数据竞争。
type Node struct {
	cfg   Config
	st    *Storage
	tr    *MemTransport
	inbox <-chan Message
	logf  func(string, ...any)

	// ---- 持久化状态 ----
	term      uint64
	votedFor  string
	entries   []LogEntry // 快照之后的日志，entries[0].Index == snapIndex+1
	snapIndex uint64
	snapTerm  uint64
	lastSnap  *SnapshotMeta // 最近一次快照（含数据，用于 InstallSnapshot）

	// ---- 易失状态 ----
	role        Role
	commitIndex uint64
	lastApplied uint64
	kv          map[string]string
	clientSeq   map[string]uint64 // 幂等表：clientID -> 已应用的最大 seq
	clientIndex map[string]uint64 // 幂等表：clientKey -> 日志 index
	leaderID    string

	// ---- 领导专用 ----
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// ---- 选举 ----
	votes           map[string]bool
	electionElapsed int
	heartbeatElapsed int
	electionTimeout int
	rng             *rand.Rand

	// ---- 客户端等待 ----
	pending map[uint64][]chan proposeRes // 日志 index -> 等待者

	proposeCh chan proposeReq
	readCh    chan readReq
	statusCh  chan chan Status
	dumpCh    chan chan []LogEntry
	stopCh    chan struct{}
	done      chan struct{}
	stopOnce  sync.Once
}

func clientKey(c Command) string {
	return fmt.Sprintf("%s/%d", c.ClientID, c.Seq)
}

// NewNode 构造节点并从磁盘恢复状态（重启场景复用同一 Dir）。
func NewNode(cfg Config, tr *MemTransport, logf func(string, ...any)) (*Node, error) {
	cfg.withDefaults()
	if logf == nil {
		logf = func(string, ...any) {}
	}
	prefix := logf
	wrapped := func(format string, args ...any) {
		prefix("["+cfg.ID+"] "+format, args...)
	}
	st, err := openStorage(cfg.Dir, wrapped)
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:         cfg,
		st:          st,
		tr:          tr,
		logf:        wrapped,
		role:        Follower,
		kv:          make(map[string]string),
		clientSeq:   make(map[string]uint64),
		clientIndex: make(map[string]uint64),
		votes:       make(map[string]bool),
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		pending:     make(map[uint64][]chan proposeRes),
		proposeCh:   make(chan proposeReq),
		readCh:      make(chan readReq),
		statusCh:    make(chan chan Status),
		dumpCh:      make(chan chan []LogEntry),
		stopCh:      make(chan struct{}),
		done:        make(chan struct{}),
		rng:         rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(cfg.ID))*7919)),
	}
	if err := n.recover(); err != nil {
		return nil, err
	}
	n.resetElectionTimeout()
	return n, nil
}

// recover 从磁盘加载硬状态、快照与 WAL，重建内存状态。
// 快照与 WAL 的写入顺序保证：恢复时看到的永远是某个一致的历史前缀，
// 不会出现被部分应用的状态。
func (n *Node) recover() error {
	hs, err := n.st.loadHardState()
	if err != nil {
		return err
	}
	n.term, n.votedFor = hs.CurrentTerm, hs.VotedFor

	snap, err := n.st.loadSnapshot()
	if err != nil {
		return err
	}
	if snap != nil {
		n.lastSnap = snap
		n.snapIndex = snap.LastIncludedIndex
		n.snapTerm = snap.LastIncludedTerm
		for k, v := range snap.Data {
			n.kv[k] = v
		}
		for k, v := range snap.ClientSeq {
			n.clientSeq[k] = v
		}
	}
	// 提交位点恢复到快照位置；快照之后的条目等待新领导重新提交。
	n.commitIndex = n.snapIndex
	n.lastApplied = n.snapIndex

	entries, err := n.st.loadEntries()
	if err != nil {
		return err
	}
	// 丢弃 WAL 中已被快照覆盖的条目（快照与 WAL 重写之间宕机可能出现重叠）。
	for _, e := range entries {
		if e.Index > n.snapIndex {
			n.entries = append(n.entries, e)
			if e.Command.ClientID != "" {
				n.clientIndex[clientKey(e.Command)] = e.Index
			}
		}
	}
	n.logf("恢复完成: term=%d votedFor=%q snapIndex=%d 日志=%d条 lastIndex=%d commit=%d",
		n.term, n.votedFor, n.snapIndex, len(n.entries), n.lastIndex(), n.commitIndex)
	return nil
}

// Start 注册收件箱并启动事件循环。
func (n *Node) Start() {
	n.inbox = n.tr.Register(n.cfg.ID)
	go n.run()
}

// Stop 停止节点（模拟宕机；持久化目录保留，可用同一 Config 重启）。
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
	<-n.done
	n.tr.Unregister(n.cfg.ID)
}

func (n *Node) run() {
	tick := time.NewTicker(time.Duration(n.cfg.TickIntervalMs) * time.Millisecond)
	defer tick.Stop()
	defer close(n.done)
	for {
		select {
		case <-n.stopCh:
			n.failAllPending(ErrStopped)
			n.logf("节点停止: term=%d commit=%d", n.term, n.commitIndex)
			return
		case <-tick.C:
			n.onTick()
		case m := <-n.inbox:
			n.onMessage(m)
		case req := <-n.proposeCh:
			n.onPropose(req)
		case req := <-n.readCh:
			v, ok := n.kv[req.key]
			req.reply <- readRes{val: v, ok: ok}
		case rc := <-n.statusCh:
			rc <- Status{Role: n.role, Term: n.term, CommitIndex: n.commitIndex,
				LastApplied: n.lastApplied, LastIndex: n.lastIndex(), LeaderID: n.leaderID}
		case rc := <-n.dumpCh:
			cp := make([]LogEntry, len(n.entries))
			copy(cp, n.entries)
			rc <- cp
		}
	}
}

func (n *Node) onMessage(m Message) {
	switch m.Kind {
	case "rv-req":
		n.onRequestVoteReq(*m.RVReq)
	case "rv-resp":
		n.onRequestVoteResp(*m.RVResp)
	case "ae-req":
		n.onAppendEntriesReq(*m.AEReq)
	case "ae-resp":
		n.onAppendEntriesResp(*m.AEResp)
	case "snap-req":
		n.onInstallSnapshotReq(*m.SnapReq)
	case "snap-resp":
		n.onInstallSnapshotResp(*m.SnapResp)
	}
}

// ---- 日志索引辅助 ----

func (n *Node) lastIndex() uint64 { return n.snapIndex + uint64(len(n.entries)) }

func (n *Node) lastTerm() uint64 { return n.termAt(n.lastIndex()) }

// termAt 返回 index 处条目的任期；index 不在可用范围时返回 0。
func (n *Node) termAt(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	if index == n.snapIndex {
		return n.snapTerm
	}
	if index < n.snapIndex || index > n.lastIndex() {
		return 0
	}
	return n.entries[index-n.snapIndex-1].Term
}

// entryAt 返回 index 处的条目。
func (n *Node) entryAt(index uint64) LogEntry {
	return n.entries[index-n.snapIndex-1]
}

// sliceFrom 返回 [from, lastIndex] 的条目副本。
func (n *Node) sliceFrom(from uint64) []LogEntry {
	if from > n.lastIndex() {
		return nil
	}
	out := make([]LogEntry, n.lastIndex()-from+1)
	copy(out, n.entries[from-n.snapIndex-1:])
	return out
}

// ---- 应用与提交 ----

// applyTo 把 (lastApplied, to] 之间的已提交条目应用到状态机。
func (n *Node) applyTo(to uint64) {
	for n.lastApplied < to {
		n.lastApplied++
		e := n.entryAt(n.lastApplied)
		cmd := e.Command
		// 幂等应用：同一客户端的重复条目只生效一次。
		dup := cmd.ClientID != "" && cmd.Seq <= n.clientSeq[cmd.ClientID]
		if !dup {
			switch cmd.Op {
			case "set":
				n.kv[cmd.Key] = cmd.Value
			case "del":
				delete(n.kv, cmd.Key)
			}
		}
		if cmd.ClientID != "" && cmd.Seq > n.clientSeq[cmd.ClientID] {
			n.clientSeq[cmd.ClientID] = cmd.Seq
		}
		if cmd.ClientID != "" {
			n.clientIndex[clientKey(cmd)] = e.Index
		}
		if cmd.Op != "noop" {
			n.logf("应用条目 index=%d term=%d op=%s key=%q dup=%v commit=%d",
				e.Index, e.Term, cmd.Op, cmd.Key, dup, n.commitIndex)
		}
		if waiters, ok := n.pending[e.Index]; ok {
			for _, w := range waiters {
				w <- proposeRes{index: e.Index}
			}
			delete(n.pending, e.Index)
		}
	}
}

func (n *Node) failAllPending(err error) {
	for idx, waiters := range n.pending {
		for _, w := range waiters {
			w <- proposeRes{err: err}
		}
		delete(n.pending, idx)
	}
}

// ---- 对外 API ----

// Propose 提交一条客户端写入。Command 必须携带 ClientID+Seq 幂等标识；
// 提交成功后返回日志 index。非领导或节点停止时返回错误，客户端可重试。
func (n *Node) Propose(cmd Command) (uint64, error) {
	req := proposeReq{cmd: cmd, reply: make(chan proposeRes, 1)}
	select {
	case n.proposeCh <- req:
	case <-n.done:
		return 0, ErrStopped
	}
	select {
	case res := <-req.reply:
		return res.index, res.err
	case <-n.done:
		return 0, ErrStopped
	}
}

// ReadCommitted 读取已提交状态机中的值。
func (n *Node) ReadCommitted(key string) (string, bool) {
	req := readReq{key: key, reply: make(chan readRes, 1)}
	select {
	case n.readCh <- req:
	case <-n.done:
		return "", false
	}
	res := <-req.reply
	return res.val, res.ok
}

// Status 返回当前角色、任期与提交位点（用于测试断言与日志）。
func (n *Node) Status() Status {
	rc := make(chan Status, 1)
	select {
	case n.statusCh <- rc:
	case <-n.done:
		return Status{}
	}
	return <-rc
}

// ID 返回节点标识。
func (n *Node) ID() string { return n.cfg.ID }

// DumpLog 返回快照之后日志段的副本（测试观测用）。
func (n *Node) DumpLog() []LogEntry {
	rc := make(chan []LogEntry, 1)
	select {
	case n.dumpCh <- rc:
	case <-n.done:
		return nil
	}
	return <-rc
}
