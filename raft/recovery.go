package raft

import (
	"encoding/json"
	"fmt"
	"strings"
)

// RecoveryReport 记录一次崩溃恢复的完整流水：回退了哪些、补齐了哪些、
// 清理了哪些残留，以及是否已恢复到可对外服务的健康状态。
// 既可直接打印（String），也可按字段程序化读取。
type RecoveryReport struct {
	NodeID string // 节点标识
	Term   int    // 恢复到的持久化任期

	// 磁盘恢复结果
	SnapshotIndex     int      // 恢复所基于的快照点（已提交前缀）
	SnapshotTerm      int      // 快照点任期
	LogKept           int      // 快照之后保留的日志条数
	CommitIndex       int      // 恢复时点的提交位点（= 快照点，其余待领导确认）
	DiskTruncatedFrom int      // 磁盘日志回退起点（损坏/不连续尾部被截断；0 表示无回退）
	ResidueCleaned    []string // 本次清理的残留文件（中断的临时文件、过期日志等）

	// 追赶结果（恢复健康时回填）
	RolledBackFrom int  // 追赶期间被领导日志覆盖而回退的起点（0 表示无）
	FilledFrom     int  // 重启后从领导补齐的日志区间起点（0 表示无需补齐）
	FilledTo       int  // 补齐区间终点
	Healthy        bool // 是否已与集群收敛、可对外提供读取与提交
}

// String 输出人可读的恢复摘要。
func (r RecoveryReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "recovery[node=%s term=%d]: snapshot=%d(term=%d) log-kept=%d commit=%d",
		r.NodeID, r.Term, r.SnapshotIndex, r.SnapshotTerm, r.LogKept, r.CommitIndex)
	if r.DiskTruncatedFrom > 0 {
		fmt.Fprintf(&b, " disk-truncated-from=%d", r.DiskTruncatedFrom)
	}
	if len(r.ResidueCleaned) > 0 {
		fmt.Fprintf(&b, " residue-cleaned=%v", r.ResidueCleaned)
	}
	if r.RolledBackFrom > 0 {
		fmt.Fprintf(&b, " rolled-back-from=%d", r.RolledBackFrom)
	}
	if r.FilledTo > 0 {
		fmt.Fprintf(&b, " filled=[%d..%d]", r.FilledFrom, r.FilledTo)
	}
	fmt.Fprintf(&b, " healthy=%v", r.Healthy)
	return b.String()
}

// recoverFromDisk 从磁盘恢复持久化状态并生成恢复报告。
// 遵循“先落盘、再生效”的逆过程：硬状态 → 快照 → 日志（清理残留并截断损坏尾部）。
// 恢复完成后节点处于非健康状态，须与集群重新收敛（见 markHealthyLocked）
// 才能对外提供读取与提交。该流程幂等：任意次数、任意位置的中断重启结果一致。
func (n *Node) recoverFromDisk() error {
	rep := RecoveryReport{NodeID: n.cfg.ID}

	// 1) 硬状态：任期与投票记录
	hs, err := n.store.LoadHardState()
	if err != nil {
		return err
	}
	n.term, n.votedFor = hs.Term, hs.VotedFor
	rep.Term = hs.Term

	// 2) 快照：唯一被视为已提交的前缀
	snapIdx, snapTerm, data, err := n.store.LoadSnapshot()
	if err != nil {
		return err
	}
	n.snapIndex, n.snapTerm = snapIdx, snapTerm
	if len(data) > 0 {
		var ms machineState
		if err := json.Unmarshal(data, &ms); err != nil {
			return fmt.Errorf("load snapshot state: %w", err)
		}
		n.kv, n.appliedSeq = ms.KV, ms.Seq
	}
	rep.SnapshotIndex, rep.SnapshotTerm = snapIdx, snapTerm

	// 3) 日志：清理中断残留，截断损坏/不连续尾部，恢复一致前缀
	entries, lrec, err := n.store.RecoverLog(snapIdx)
	if err != nil {
		return err
	}
	n.entries = entries
	rep.LogKept = len(entries)
	rep.DiskTruncatedFrom = lrec.TruncatedFrom
	rep.ResidueCleaned = lrec.Removed

	// 快照之后的条目一律视为未提交：等领导心跳重新确认提交位点后幂等重放，
	// 因此中断不会留下被部分应用的状态。
	n.commitIndex = snapIdx
	n.lastApplied = snapIdx
	rep.CommitIndex = snapIdx

	n.healthy = false
	n.lastRecovery = rep
	n.logf("recovered: %s", rep.String())
	return nil
}

// LastRecovery 返回最近一次崩溃恢复的报告（恢复健康后包含追赶结果）。
func (n *Node) LastRecovery() RecoveryReport {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastRecovery
}

// Healthy 报告节点是否已恢复到可对外服务的一致状态：
// 重启后必须与集群重新收敛（跟随者收齐领导确认并应用完毕，
// 或领导在本任期提交条目）之后才置为 true。
func (n *Node) Healthy() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.healthy
}

// markHealthyLocked 将节点标记为健康并回填恢复报告的追赶结果（持锁调用）。
func (n *Node) markHealthyLocked() {
	if n.healthy {
		return
	}
	n.healthy = true
	if n.lastRecovery.FilledFrom > 0 || n.lastIndexLocked() > n.lastRecovery.SnapshotIndex {
		if n.lastRecovery.FilledFrom == 0 {
			n.lastRecovery.FilledFrom = n.lastRecovery.SnapshotIndex + 1
		}
		n.lastRecovery.FilledTo = n.lastIndexLocked()
	}
	n.lastRecovery.Healthy = true
	n.logf("healthy again: %s", n.lastRecovery.String())
}
