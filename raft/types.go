// Package raft 实现一个单机多进程内模拟的一致性集群（Raft 风格）。
// 通信、宕机、重启均通过内存 Transport 与文件持久化模拟，不依赖外部服务。
package raft

// Role 节点角色。
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// Command 是状态机命令。ClientID+Seq 构成幂等标识，超时重试不会产生重复条目。
type Command struct {
	Op       string `json:"op"` // "set" / "del"
	Key      string `json:"key"`
	Value    string `json:"value"`
	ClientID string `json:"client_id"`
	Seq      uint64 `json:"seq"`
}

// LogEntry 日志条目。
type LogEntry struct {
	Term    uint64  `json:"term"`
	Index   uint64  `json:"index"`
	Command Command `json:"command"`
}

// HardState 需要持久化的选举状态。
type HardState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"` // 空表示未投票
}

// SnapshotMeta 快照元数据。
type SnapshotMeta struct {
	LastIncludedIndex uint64            `json:"last_included_index"`
	LastIncludedTerm  uint64            `json:"last_included_term"`
	Data              map[string]string `json:"data"`             // KV 状态机数据
	ClientSeq         map[string]uint64 `json:"client_seq"`       // 幂等表随快照一起持久化
}

// ---- RPC 消息 ----

type RequestVoteReq struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type RequestVoteResp struct {
	Term        uint64 `json:"term"`
	Granted     bool   `json:"granted"`
	From        string `json:"from"`
}

type AppendEntriesReq struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

type AppendEntriesResp struct {
	Term    uint64 `json:"term"`
	From    string `json:"from"`
	Success bool   `json:"success"`
	// 冲突快速回退信息
	ConflictTerm  uint64 `json:"conflict_term"`
	ConflictIndex uint64 `json:"conflict_index"`
	MatchIndex    uint64 `json:"match_index"`
}

type InstallSnapshotReq struct {
	Term              uint64            `json:"term"`
	LeaderID          string            `json:"leader_id"`
	LastIncludedIndex uint64            `json:"last_included_index"`
	LastIncludedTerm  uint64            `json:"last_included_term"`
	Data              map[string]string `json:"data"`
	ClientSeq         map[string]uint64 `json:"client_seq"`
}

type InstallSnapshotResp struct {
	Term uint64 `json:"term"`
	From string `json:"from"`
}

// Message 是 Transport 上传输的信封。
type Message struct {
	From    string
	To      string
	Kind    string // "rv-req","rv-resp","ae-req","ae-resp","snap-req","snap-resp"
	RVReq   *RequestVoteReq
	RVResp  *RequestVoteResp
	AEReq   *AppendEntriesReq
	AEResp  *AppendEntriesResp
	SnapReq *InstallSnapshotReq
	SnapResp *InstallSnapshotResp
}

// Config 可配置项。
type Config struct {
	ID                string // 节点 ID
	Peers             []string
	Dir               string // 持久化目录
	ElectionTickMin   int    // 选举超时（tick 数）下界
	ElectionTickMax   int    // 选举超时（tick 数）上界
	HeartbeatTick     int    // 心跳间隔（tick 数）
	TickIntervalMs    int    // 一个 tick 的真实时长（毫秒）
	SnapshotThreshold int    // 日志条数超过该值触发快照压缩
}

func (c *Config) withDefaults() {
	if c.ElectionTickMin == 0 {
		c.ElectionTickMin = 15
	}
	if c.ElectionTickMax == 0 {
		c.ElectionTickMax = 30
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 3
	}
	if c.TickIntervalMs == 0 {
		c.TickIntervalMs = 10
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 50
	}
}
