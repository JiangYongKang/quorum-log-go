// Package raft 实现一套可在单机多进程内模拟的一致性集群（Raft 变体）。
// 节点通过内存网络通信，通过本地文件持久化，支持领导选举、日志复制、
// 快照压缩与崩溃恢复。
package raft

// Role 表示节点角色。
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

// LogEntry 是一条日志。ClientID+Seq 构成幂等标识，重试不会产生重复条目。
type LogEntry struct {
	Term     int    // 写入该条目的领导任期
	Index    int    // 日志下标（从 1 开始）
	ClientID string // 客户端标识
	Seq      int64  // 客户端单调递增序号
	Command  []byte // 状态机命令
}

// RequestVote 请求投票 RPC。
type RequestVote struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term    int
	Granted bool
	From    string
}

// AppendEntries 日志复制/心跳 RPC。Snapshot 非空时表示快照安装。
type AppendEntries struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	From    string
	// 冲突提示：用于快速回退 nextIndex
	ConflictTerm  int
	ConflictIndex int
	MatchIndex    int // 接收方当前日志末尾下标（含快照）
}

// InstallSnapshot 快照安装 RPC。
type InstallSnapshot struct {
	Term              int
	LeaderID          string
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte // 状态机快照字节流
}

type InstallSnapshotReply struct {
	Term int
	From string
}

// ClientWrite 客户端写请求。
type ClientWrite struct {
	ClientID string
	Seq      int64
	Command  []byte
}

// ClientReply 客户端响应。
type ClientReply struct {
	OK        bool
	Committed bool
	LeaderID  string // 非领导时提示当前已知领导
	Index     int    // 已提交条目的下标
	Err       string
}
