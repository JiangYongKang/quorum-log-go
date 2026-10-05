package raft

import "fmt"

// 崩溃注入阶段（故障注入点，模拟进程在特定步骤之间被强杀/掉电）。
const (
	CrashNone             = ""                 // 不注入
	CrashAfterTmpWrite    = "tmp"              // 临时文件已写盘并 fsync、rename 之前
	CrashSnapshotCompact  = "snapshot-compact" // 快照已原子替换、已覆盖日志删除之前
	CrashMidTruncate      = "truncate"         // 后缀截断删到一半
	CrashMidAppend        = "append"           // 批量追加日志写到一半
)

// ResidueInfo 记录一次启动恢复中清理掉的崩溃残留。
type ResidueInfo struct {
	TempFiles []string // 删除的临时文件（相对数据目录的路径）
	StaleLogs []int    // 已被快照覆盖、删除的日志下标
	Truncated []int    // 因空洞/损坏被截断的日志下标
}

// RecoveryReport 是节点一次“崩溃恢复 + 追赶收敛”的完整记录，
// 既可 JSON 序列化供程序读取，也可通过 String 给人看。
type RecoveryReport struct {
	NodeID           string   `json:"nodeID"`
	Term             int      `json:"term"`             // 恢复出的持久化任期
	SnapshotIndex    int      `json:"snapshotIndex"`    // 恢复所基于的快照位点
	SnapshotTerm     int      `json:"snapshotTerm"`
	LoadedEntries    int      `json:"loadedEntries"`    // 恢复出的有效日志条数
	RolledBack       []int    `json:"rolledBack,omitempty"`       // 回退（截断）的日志下标
	StaleLogsRemoved []int    `json:"staleLogsRemoved,omitempty"` // 清理的、快照已覆盖的日志下标
	TempFilesRemoved []string `json:"tempFilesRemoved,omitempty"` // 清理的临时文件
	CaughtUpFrom     int      `json:"caughtUpFrom"` // 追赶起点（恢复完成时的已提交位点）
	CaughtUpTo       int      `json:"caughtUpTo"`   // 恢复健康时已应用到的位点
	Ready            bool     `json:"ready"`        // 是否已恢复到可对外服务的健康状态
}

// String 输出人类可读的恢复摘要。
func (r RecoveryReport) String() string {
	return fmt.Sprintf("recovery(node=%s term=%d snap=%d loaded=%d rolledBack=%v staleLogs=%v tempFiles=%v caughtUp=%d->%d ready=%v)",
		r.NodeID, r.Term, r.SnapshotIndex, r.LoadedEntries,
		r.RolledBack, r.StaleLogsRemoved, r.TempFilesRemoved,
		r.CaughtUpFrom, r.CaughtUpTo, r.Ready)
}
