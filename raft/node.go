package raft

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"math/rand"
	"strings"
	"sync"
)

// Config 节点配置。
type Config struct {
	ID                string
	Peers             []string // 含自身的全部节点 ID
	DataDir           string
	ElectionTimeout   int // 选举超时基数（tick 数），实际超时为 [T, 2T) 随机
	HeartbeatTick     int // 心跳间隔（tick 数）
	SnapshotThreshold int // 未压缩日志条数超过该值时触发快照
}

func (c *Config) defaults() {
	if c.ElectionTimeout <= 0 {
		c.ElectionTimeout = 10
	}
	if c.HeartbeatTick <= 0 {
		c.HeartbeatTick = 2
	}
	if c.SnapshotThreshold <= 0 {
		c.SnapshotThreshold = 50
	}
}

// 状态机快照数据：KV 与客户端幂等序号一起持久化，保证恢复后语义一致。
type machineState struct {
	KV  map[string]string `json:"kv"`
	Seq map[string]int64  `json:"seq"`
}

// Node 是集群中的一个节点。所有状态变更遵循“先落盘、再生效”的顺序。
type Node struct {
	cfg Config
	net *Network
	log *log.Logger // 可注入，测试用来打印任期/提交位点/落盘流水

	mu       sync.Mutex
	running  bool
	role     Role
	term     int
	votedFor string

	// 日志与快照
	store     *Storage
	entries   []LogEntry // 仅含快照之后的条目，entries[i].Index == snapIndex+1+i
	snapIndex int
	snapTerm  int

	// 提交与应用
	commitIndex int
	lastApplied int
	kv          map[string]string
	appliedSeq  map[string]int64 // 客户端幂等表（已应用的最大 Seq）

	// 崩溃恢复：重启后须先与集群收敛（healthy）才能对外提供读取与提交
	healthy      bool
	lastRecovery RecoveryReport

	// 选举计时
	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int
	rand             *rand.Rand

	// 领导视图
	nextIndex  map[string]int
	matchIndex map[string]int
}

// NewNode 创建节点并从磁盘恢复持久化状态。
func NewNode(cfg Config, net *Network, logger *log.Logger) (*Node, error) {
	cfg.defaults()
	store, err := OpenStorage(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:        cfg,
		net:        net,
		log:        logger,
		store:      store,
		role:       Follower,
		kv:         map[string]string{},
		appliedSeq: map[string]int64{},
		nextIndex:  map[string]int{},
		matchIndex: map[string]int{},
	}
	h := fnv.New64a()
	h.Write([]byte(cfg.ID))
	n.rand = rand.New(rand.NewSource(int64(h.Sum64())))
	n.resetElectionTimeoutLocked()

	// 从磁盘恢复持久化状态（含残留清理与损坏尾部截断），生成恢复报告。
	// 恢复后节点处于非健康状态，须与集群重新收敛后才能对外服务。
	if err := n.recoverFromDisk(); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *Node) logf(format string, args ...any) {
	if n.log != nil {
		n.log.Printf("[node %s term=%d role=%s commit=%d] "+format,
			append([]any{n.cfg.ID, n.term, n.role, n.commitIndex}, args...)...)
	}
}

// Start 启动节点（注册到网络）。
func (n *Node) Start() {
	n.mu.Lock()
	n.running = true
	n.mu.Unlock()
	n.net.Register(n.cfg.ID, n)
}

// Stop 停止节点（模拟宕机，持久化数据保留在磁盘）。
func (n *Node) Stop() {
	n.net.Unregister(n.cfg.ID)
	n.mu.Lock()
	n.running = false
	n.mu.Unlock()
}

// ID 返回节点标识。
func (n *Node) ID() string { return n.cfg.ID }

// ---- 日志索引辅助（持锁调用） ----

func (n *Node) lastIndexLocked() int {
	if len(n.entries) == 0 {
		return n.snapIndex
	}
	return n.entries[len(n.entries)-1].Index
}

func (n *Node) lastTermLocked() int {
	return n.termAtLocked(n.lastIndexLocked())
}

func (n *Node) termAtLocked(index int) int {
	switch {
	case index == n.snapIndex:
		return n.snapTerm
	case index < n.snapIndex:
		return 0
	case index-n.snapIndex-1 < len(n.entries):
		return n.entries[index-n.snapIndex-1].Term
	}
	return 0
}

func (n *Node) entryAtLocked(index int) LogEntry {
	return n.entries[index-n.snapIndex-1]
}

func (n *Node) entriesFromLocked(index int) []LogEntry {
	if index <= n.snapIndex {
		out := make([]LogEntry, len(n.entries))
		copy(out, n.entries)
		return out
	}
	off := index - n.snapIndex - 1
	if off >= len(n.entries) {
		return nil
	}
	out := make([]LogEntry, len(n.entries)-off)
	copy(out, n.entries[off:])
	return out
}

func (n *Node) majority() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) resetElectionTimeoutLocked() {
	n.electionElapsed = 0
	n.electionTimeout = n.cfg.ElectionTimeout + n.rand.Intn(n.cfg.ElectionTimeout)
}

// ---- 时钟驱动 ----

// Tick 驱动节点时钟（由运行器逐个调用，行为确定可复现）。
func (n *Node) Tick() {
	n.mu.Lock()
	if !n.running {
		n.mu.Unlock()
		return
	}
	if n.role == Leader {
		n.heartbeatElapsed++
		fire := n.heartbeatElapsed >= n.cfg.HeartbeatTick
		if fire {
			n.heartbeatElapsed = 0
		}
		n.mu.Unlock()
		if fire {
			n.broadcastAppend()
		}
		return
	}
	n.electionElapsed++
	timeout := n.electionElapsed >= n.electionTimeout
	n.mu.Unlock()
	if timeout {
		n.startElection()
	}
}

// ---- 选举 ----

func (n *Node) startElection() {
	n.mu.Lock()
	if !n.running || n.role == Leader {
		n.mu.Unlock()
		return
	}
	n.role = Candidate
	n.term++
	n.votedFor = n.cfg.ID
	if err := n.store.SaveHardState(hardState{Term: n.term, VotedFor: n.votedFor}); err != nil {
		n.logf("persist hardstate failed: %v", err)
	}
	n.resetElectionTimeoutLocked()
	term := n.term
	req := RequestVote{
		Term:         term,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.lastIndexLocked(),
		LastLogTerm:  n.lastTermLocked(),
	}
	votes := 1
	n.logf("start election term=%d lastIndex=%d", term, req.LastLogIndex)
	n.mu.Unlock()

	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.ID {
			continue
		}
		reply, ok := n.net.SendVote(n.cfg.ID, peer, req)
		if !ok {
			continue
		}
		n.mu.Lock()
		if n.term != term || n.role != Candidate {
			n.mu.Unlock()
			return
		}
		if reply.Term > n.term {
			n.stepDownLocked(reply.Term)
			n.mu.Unlock()
			return
		}
		if reply.Granted {
			votes++
		}
		win := votes >= n.majority()
		n.mu.Unlock()
		if win {
			n.becomeLeader(term)
			return
		}
	}
}

func (n *Node) becomeLeader(term int) {
	n.mu.Lock()
	if !n.running || n.role != Candidate || n.term != term {
		n.mu.Unlock()
		return
	}
	n.role = Leader
	n.heartbeatElapsed = 0
	last := n.lastIndexLocked()
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.cfg.ID] = last
	n.logf("become leader term=%d", term)
	// 追加一条 no-op，使本任期尽快有可提交条目，从而能推进之前任期的提交位点。
	noop := LogEntry{Term: term, Index: last + 1}
	n.appendEntriesLocked([]LogEntry{noop})
	n.matchIndex[n.cfg.ID] = noop.Index
	n.mu.Unlock()
	n.broadcastAppend()
}

// stepDownLocked 发现更高任期时退为跟随者（持锁调用）。
func (n *Node) stepDownLocked(newTerm int) {
	if newTerm > n.term {
		n.term = newTerm
		n.votedFor = ""
		if err := n.store.SaveHardState(hardState{Term: n.term, VotedFor: ""}); err != nil {
			n.logf("persist hardstate failed: %v", err)
		}
	}
	n.role = Follower
	n.resetElectionTimeoutLocked()
}

// HandleVote 处理投票请求（由网络层调用）。
func (n *Node) HandleVote(req RequestVote) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := RequestVoteReply{Term: n.term, From: n.cfg.ID}
	if req.Term < n.term {
		return reply
	}
	if req.Term > n.term {
		n.stepDownLocked(req.Term)
	}
	upToDate := req.LastLogTerm > n.lastTermLocked() ||
		(req.LastLogTerm == n.lastTermLocked() && req.LastLogIndex >= n.lastIndexLocked())
	if (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate {
		n.votedFor = req.CandidateID
		n.resetElectionTimeoutLocked()
		if err := n.store.SaveHardState(hardState{Term: n.term, VotedFor: n.votedFor}); err != nil {
			n.logf("persist hardstate failed: %v", err)
		}
		reply.Granted = true
		n.logf("grant vote to %s term=%d", req.CandidateID, req.Term)
	} else {
		n.logf("reject vote to %s term=%d votedFor=%q upToDate=%v",
			req.CandidateID, req.Term, n.votedFor, upToDate)
	}
	reply.Term = n.term
	return reply
}

// ---- 日志复制（领导侧） ----

// appendEntriesLocked 追加本地条目并落盘（持锁调用）。
func (n *Node) appendEntriesLocked(entries []LogEntry) {
	if err := n.store.AppendLog(entries); err != nil {
		n.logf("persist log failed: %v", err)
		return
	}
	n.entries = append(n.entries, entries...)
	for _, e := range entries {
		n.logf("persist log index=%d term=%d client=%q seq=%d", e.Index, e.Term, e.ClientID, e.Seq)
	}
}

// broadcastAppend 向所有跟随者发送 AppendEntries 或 InstallSnapshot。
func (n *Node) broadcastAppend() {
	n.mu.Lock()
	if !n.running || n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.term
	type job struct {
		peer    string
		append  *AppendEntries
		snapReq *InstallSnapshot
	}
	jobs := []job{}
	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.ID {
			continue
		}
		next := n.nextIndex[peer]
		if next <= n.snapIndex {
			data, _ := n.snapshotDataLocked()
			jobs = append(jobs, job{peer: peer, snapReq: &InstallSnapshot{
				Term:              term,
				LeaderID:          n.cfg.ID,
				LastIncludedIndex: n.snapIndex,
				LastIncludedTerm:  n.snapTerm,
				Data:              data,
			}})
			continue
		}
		jobs = append(jobs, job{peer: peer, append: &AppendEntries{
			Term:         term,
			LeaderID:     n.cfg.ID,
			PrevLogIndex: next - 1,
			PrevLogTerm:  n.termAtLocked(next - 1),
			Entries:      n.entriesFromLocked(next),
			LeaderCommit: n.commitIndex,
		}})
	}
	n.mu.Unlock()

	for _, j := range jobs {
		if j.snapReq != nil {
			n.sendSnapshot(j.peer, term, j.snapReq)
		} else {
			n.sendAppend(j.peer, term, j.append)
		}
	}
	n.mu.Lock()
	n.advanceCommitLocked(term)
	n.mu.Unlock()
}

func (n *Node) sendAppend(peer string, term int, req *AppendEntries) {
	reply, ok := n.net.SendAppend(n.cfg.ID, peer, *req)
	if !ok {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.term != term || n.role != Leader {
		return
	}
	if reply.Term > n.term {
		n.stepDownLocked(reply.Term)
		return
	}
	if reply.Success {
		n.matchIndex[peer] = req.PrevLogIndex + len(req.Entries)
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		return
	}
	// 冲突回退：利用跟随者返回的任期提示快速定位。
	next := n.nextIndex[peer]
	if reply.ConflictTerm > 0 {
		// 若本地有该任期的条目，退到该任期最后一条之后；否则退到冲突起点。
		idx := reply.ConflictIndex
		for i := reply.ConflictIndex; i <= n.lastIndexLocked(); i++ {
			if n.termAtLocked(i) == reply.ConflictTerm {
				idx = i + 1
			}
		}
		next = idx
	} else if reply.ConflictIndex > 0 {
		next = reply.ConflictIndex
	} else {
		next--
	}
	if next < 1 {
		next = 1
	}
	if next < n.nextIndex[peer] {
		n.nextIndex[peer] = next
	}
}

func (n *Node) sendSnapshot(peer string, term int, req *InstallSnapshot) {
	n.logf("send snapshot to %s lastIncludedIndex=%d", peer, req.LastIncludedIndex)
	reply, ok := n.net.SendSnapshot(n.cfg.ID, peer, *req)
	if !ok {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.term != term || n.role != Leader {
		return
	}
	if reply.Term > n.term {
		n.stepDownLocked(reply.Term)
		return
	}
	n.matchIndex[peer] = req.LastIncludedIndex
	n.nextIndex[peer] = req.LastIncludedIndex + 1
}

// advanceCommitLocked 只提交本任期的条目（多数派确认），间接带动之前任期条目提交。
func (n *Node) advanceCommitLocked(term int) {
	if n.role != Leader || n.term != term {
		return
	}
	for i := n.commitIndex + 1; i <= n.lastIndexLocked(); i++ {
		if n.termAtLocked(i) != n.term {
			continue
		}
		count := 1
		for _, p := range n.cfg.Peers {
			if p != n.cfg.ID && n.matchIndex[p] >= i {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = i
		}
	}
	n.applyLocked()
	// 本任期条目已被多数派提交：领导自身与集群收敛，恢复健康
	if !n.healthy && n.commitIndex > 0 && n.termAtLocked(n.commitIndex) == n.term {
		n.markHealthyLocked()
	}
}

// ---- 日志复制（跟随者侧） ----

// HandleAppend 处理日志复制/心跳。
func (n *Node) HandleAppend(req AppendEntries) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := AppendEntriesReply{Term: n.term, From: n.cfg.ID, MatchIndex: n.lastIndexLocked()}
	if req.Term < n.term {
		return reply
	}
	if req.Term > n.term || n.role != Follower {
		n.stepDownLocked(req.Term)
	}
	n.resetElectionTimeoutLocked()

	// prevLog 一致性检查
	switch {
	case req.PrevLogIndex < n.snapIndex:
		// 领导发的太旧，让它从快照之后开始发
		reply.ConflictIndex = n.snapIndex + 1
		reply.Term = n.term
		return reply
	case req.PrevLogIndex > n.lastIndexLocked():
		reply.ConflictIndex = n.lastIndexLocked() + 1
		reply.Term = n.term
		return reply
	case n.termAtLocked(req.PrevLogIndex) != req.PrevLogTerm:
		conflictTerm := n.termAtLocked(req.PrevLogIndex)
		reply.ConflictTerm = conflictTerm
		first := req.PrevLogIndex
		for first-1 > n.snapIndex && n.termAtLocked(first-1) == conflictTerm {
			first--
		}
		reply.ConflictIndex = first
		reply.Term = n.term
		return reply
	}

	// 合并条目：截断冲突后缀后追加新条目
	for i, e := range req.Entries {
		exist := n.termAtLocked(e.Index)
		if exist == 0 || exist != e.Term {
			truncFrom := e.Index
			newEntries := req.Entries[i:]
			if truncFrom <= n.lastIndexLocked() {
				if err := n.store.TruncateSuffix(truncFrom); err != nil {
					n.logf("truncate failed: %v", err)
				}
				n.entries = n.entries[:truncFrom-n.snapIndex-1]
				n.logf("truncate suffix from index=%d", truncFrom)
				// 恢复期间的回退记入恢复报告
				if !n.healthy && n.lastRecovery.RolledBackFrom == 0 {
					n.lastRecovery.RolledBackFrom = truncFrom
				}
			}
			n.appendEntriesLocked(newEntries)
			break
		}
	}

	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, n.lastIndexLocked())
	}
	n.applyLocked()
	// 已按领导确认的提交位点应用完毕：与集群收敛，恢复健康
	if !n.healthy && n.lastApplied >= n.commitIndex {
		n.markHealthyLocked()
	}
	reply.Success = true
	reply.MatchIndex = n.lastIndexLocked()
	reply.Term = n.term
	return reply
}

// HandleSnapshot 处理快照安装：先原子落盘，再整体替换内存状态，中断不留半成品。
func (n *Node) HandleSnapshot(req InstallSnapshot) InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := InstallSnapshotReply{Term: n.term, From: n.cfg.ID}
	if req.Term < n.term {
		return reply
	}
	if req.Term > n.term || n.role != Follower {
		n.stepDownLocked(req.Term)
	}
	n.resetElectionTimeoutLocked()
	if req.LastIncludedIndex <= n.snapIndex {
		return reply
	}
	// 1) 快照原子落盘（含日志压缩）
	if err := n.store.SaveSnapshot(req.LastIncludedIndex, req.LastIncludedTerm, req.Data); err != nil {
		n.logf("persist snapshot failed: %v", err)
		return reply
	}
	// 2) 校验并解码状态机数据
	var ms machineState
	if err := json.Unmarshal(req.Data, &ms); err != nil {
		n.logf("bad snapshot data: %v", err)
		return reply
	}
	// 3) 整体替换内存状态
	keep := []LogEntry{}
	if n.termAtLocked(req.LastIncludedIndex) == req.LastIncludedTerm {
		keep = n.entriesFromLocked(req.LastIncludedIndex + 1)
	}
	n.entries = keep
	n.snapIndex = req.LastIncludedIndex
	n.snapTerm = req.LastIncludedTerm
	n.kv, n.appliedSeq = ms.KV, ms.Seq
	if n.commitIndex < n.snapIndex {
		n.commitIndex = n.snapIndex
	}
	n.lastApplied = n.snapIndex
	n.logf("install snapshot lastIncludedIndex=%d lastIncludedTerm=%d",
		req.LastIncludedIndex, req.LastIncludedTerm)
	// 快照整体替换后，已应用状态与领导确认的一致前缀对齐：恢复健康
	if !n.healthy && n.lastApplied >= n.commitIndex {
		n.markHealthyLocked()
	}
	return reply
}

// ---- 应用与快照 ----

// applyLocked 将已提交条目应用到状态机（持锁调用）。应用按客户端 Seq 幂等。
func (n *Node) applyLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e := n.entryAtLocked(n.lastApplied)
		if e.ClientID != "" && e.Seq > n.appliedSeq[e.ClientID] {
			n.applyCommandLocked(e.Command)
			n.appliedSeq[e.ClientID] = e.Seq
		}
		n.logf("apply index=%d term=%d", e.Index, e.Term)
	}
	n.maybeSnapshotLocked()
}

// applyCommandLocked 执行状态机命令："set:key:value" 或 "del:key"。
func (n *Node) applyCommandLocked(cmd []byte) {
	parts := strings.SplitN(string(cmd), ":", 3)
	if len(parts) == 3 && parts[0] == "set" {
		n.kv[parts[1]] = parts[2]
	} else if len(parts) == 2 && parts[0] == "del" {
		delete(n.kv, parts[1])
	}
}

// snapshotDataLocked 序列化当前状态机（持锁调用）。
func (n *Node) snapshotDataLocked() ([]byte, error) {
	return json.Marshal(machineState{KV: n.kv, Seq: n.appliedSeq})
}

// maybeSnapshotLocked 未压缩日志超过阈值时对已应用前缀做快照（持锁调用）。
func (n *Node) maybeSnapshotLocked() {
	if n.lastApplied-n.snapIndex < n.cfg.SnapshotThreshold {
		return
	}
	data, err := n.snapshotDataLocked()
	if err != nil {
		n.logf("snapshot encode failed: %v", err)
		return
	}
	index, term := n.lastApplied, n.termAtLocked(n.lastApplied)
	if err := n.store.SaveSnapshot(index, term, data); err != nil {
		n.logf("persist snapshot failed: %v", err)
		return
	}
	// 压缩内存中的日志前缀
	n.entries = n.entriesFromLocked(index + 1)
	n.snapIndex, n.snapTerm = index, term
	n.logf("snapshot saved lastIncludedIndex=%d lastIncludedTerm=%d", index, term)
}

// ---- 客户端接口 ----

// Write 处理客户端写入。仅领导可写；ClientID+Seq 幂等，重试不产生重复条目。
// 同步复制到达多数派并提交后才返回 Committed=true。
func (n *Node) Write(req ClientWrite) ClientReply {
	n.mu.Lock()
	if n.role != Leader {
		leader := n.knownLeaderLocked()
		n.mu.Unlock()
		return ClientReply{OK: false, LeaderID: leader, Err: "not leader"}
	}
	// 门控：重启后须先与集群收敛（本任期有条目被多数派提交）才能对外提交。
	if !n.healthy {
		n.mu.Unlock()
		n.broadcastAppend() // 促使本任期 no-op 尽快提交以恢复健康
		return ClientReply{OK: false, LeaderID: n.cfg.ID, Err: "leader recovering, retry"}
	}
	// 幂等：已应用过的请求直接返回成功
	if req.Seq <= n.appliedSeq[req.ClientID] {
		reply := ClientReply{OK: true, Committed: true, LeaderID: n.cfg.ID}
		n.mu.Unlock()
		return reply
	}
	// 幂等：日志中已有同 ClientID+Seq 的未提交条目，复用其下标继续提交
	index := -1
	for i := n.lastIndexLocked(); i > n.snapIndex; i-- {
		e := n.entryAtLocked(i)
		if e.ClientID == req.ClientID && e.Seq == req.Seq && e.Term == n.term {
			index = i
			break
		}
	}
	if index < 0 {
		entry := LogEntry{
			Term:     n.term,
			Index:    n.lastIndexLocked() + 1,
			ClientID: req.ClientID,
			Seq:      req.Seq,
			Command:  req.Command,
		}
		n.appendEntriesLocked([]LogEntry{entry})
		n.matchIndex[n.cfg.ID] = entry.Index
		index = entry.Index
	}
	term := n.term
	n.mu.Unlock()

	n.broadcastAppend()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.term != term {
		return ClientReply{OK: false, Err: "lost leadership"}
	}
	if n.commitIndex >= index {
		return ClientReply{OK: true, Committed: true, LeaderID: n.cfg.ID, Index: index}
	}
	return ClientReply{OK: true, Committed: false, LeaderID: n.cfg.ID, Index: index}
}

// knownLeaderLocked 返回当前已知领导（简化实现：跟随者不跟踪，仅领导返回自己）。
func (n *Node) knownLeaderLocked() string { return "" }

// ReadCommitted 读取已提交状态机内容。
// 仅健康状态的领导响应：重启后未与集群重新收敛的节点不对外提供读取，
// 保证读到的始终是多数派已提交且已完整应用的状态。
func (n *Node) ReadCommitted() (map[string]string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || !n.healthy {
		return nil, false
	}
	out := make(map[string]string, len(n.kv))
	for k, v := range n.kv {
		out[k] = v
	}
	return out, true
}

// AppliedKV 返回节点本地已应用状态（测试观测用）。
func (n *Node) AppliedKV() map[string]string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[string]string, len(n.kv))
	for k, v := range n.kv {
		out[k] = v
	}
	return out
}

// Status 返回可观测状态（测试与日志用）。
func (n *Node) Status() (role Role, term, commitIndex, lastIndex int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role, n.term, n.commitIndex, n.lastIndexLocked()
}
