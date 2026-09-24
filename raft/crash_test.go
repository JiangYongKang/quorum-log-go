package raft

import (
	"testing"
	"time"
)

// TestCrashRestart 验证：节点宕机重启后，
//  1. 持久化的任期/投票不丢失（同任期不重复投票）；
//  2. 已提交数据不丢失，重启后通过增量日志追赶到最新提交位点。
func TestCrashRestart(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	leader := c.WaitLeader(5 * time.Second)
	if leader == nil {
		t.Fatal("未选出领导")
	}
	for i := 1; i <= 3; i++ {
		mustPropose(t, c, Command{Op: "set", Key: "k", Value: string(rune('0' + i)), ClientID: "c", Seq: uint64(i)})
	}
	if !c.WaitCommitAll(4, 5*time.Second) {
		t.Fatal("提交未收敛")
	}

	// 找一个跟随者宕机。
	var followerID string
	for _, id := range c.IDs {
		if id != leader.ID() {
			followerID = id
			break
		}
	}
	c.Crash(followerID)

	// 宕机期间继续写入。
	for i := 4; i <= 6; i++ {
		mustPropose(t, c, Command{Op: "set", Key: "k", Value: string(rune('0' + i)), ClientID: "c", Seq: uint64(i)})
	}

	// 重启，应通过增量日志追赶。
	if err := c.Restart(followerID); err != nil {
		t.Fatalf("重启失败: %v", err)
	}
	if !c.WaitCommit(followerID, 7, 5*time.Second) {
		s := c.Nodes[followerID].Status()
		t.Fatalf("重启节点未追赶到 commit=7, 当前 commit=%d last=%d", s.CommitIndex, s.LastIndex)
	}
	if v, _ := c.Nodes[followerID].ReadCommitted("k"); v != "6" {
		t.Fatalf("重启节点读到 k=%q，期望 6", v)
	}

	// 领导宕机：集群应重新选主且已提交数据不丢。
	oldLeader := c.Leader()
	if oldLeader == nil {
		t.Fatal("重启后未收敛出唯一领导")
	}
	oldTerm := oldLeader.Status().Term
	c.Crash(oldLeader.ID())
	var nl *Node
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l != nil {
			nl = l
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if nl == nil {
		t.Fatal("领导宕机后未选出新领导")
	}
	if nl.Status().Term <= oldTerm {
		t.Fatalf("新领导任期 %d 未超过旧任期 %d", nl.Status().Term, oldTerm)
	}
	if v, _ := nl.ReadCommitted("k"); v != "6" {
		t.Fatalf("新领导丢失已提交数据: k=%q", v)
	}
	t.Logf("宕机重启测试通过: 新领导=%s term=%d", nl.ID(), nl.Status().Term)
}

// TestLogConflictOverwrite 验证：少数派节点上未提交的冲突条目，
// 会被新领导的日志覆盖，且永远不会呈现为已提交。
func TestLogConflictOverwrite(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	leader := c.WaitLeader(5 * time.Second)
	if leader == nil {
		t.Fatal("未选出领导")
	}
	mustPropose(t, c, Command{Op: "set", Key: "base", Value: "0", ClientID: "c", Seq: 1})

	// 隔离领导与另一节点形成少数派？这里隔离领导一人：
	// 老领导在分区中写入的条目无法获多数，属于未提交条目。
	oldID := leader.ID()
	c.Partition(oldID)

	// 多数派选新领导并提交新条目。
	var newLeader *Node
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.Nodes {
			if n.ID() != oldID && n.Status().Role == Leader {
				newLeader = n
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("多数派未选出新领导")
	}
	idx := mustPropose(t, c, Command{Op: "set", Key: "main", Value: "1", ClientID: "c", Seq: 2})
	mustPropose(t, c, Command{Op: "set", Key: "main2", Value: "2", ClientID: "c", Seq: 3})

	// 老领导在隔离期间累积了未提交条目（客户端重试写入）。
	old := c.Nodes[oldID]
	go old.Propose(Command{Op: "set", Key: "stale", Value: "x", ClientID: "c", Seq: 50})
	time.Sleep(300 * time.Millisecond)
	oldLast := old.Status().LastIndex
	t.Logf("老领导隔离期间 lastIndex=%d commit=%d", oldLast, old.Status().CommitIndex)

	// 恢复分区：老领导退位，其未提交条目被覆盖。
	c.Heal()
	if !c.WaitCommitAll(idx+1, 5*time.Second) {
		t.Fatal("恢复后提交位点未收敛")
	}
	time.Sleep(300 * time.Millisecond)

	// 老领导的日志应与多数派一致：冲突条目被覆盖。
	ol := c.Nodes[oldID].DumpLog()
	nl := c.Nodes[newLeader.ID()].DumpLog()
	if len(ol) != len(nl) {
		t.Fatalf("日志长度不一致: 老领导 %d, 新领导 %d", len(ol), len(nl))
	}
	for i := range ol {
		if ol[i].Term != nl[i].Term || ol[i].Command != nl[i].Command {
			t.Fatalf("index=%d 处日志不一致: 老领导 %+v, 新领导 %+v", ol[i].Index, ol[i], nl[i])
		}
	}
	// 被覆盖的 stale 条目不得呈现为已提交。
	for _, id := range c.IDs {
		if v, _ := c.Nodes[id].ReadCommitted("stale"); v != "" {
			t.Fatalf("节点 %s 将被覆盖的未提交条目呈现为已提交: stale=%q", id, v)
		}
		if v, _ := c.Nodes[id].ReadCommitted("main2"); v != "2" {
			t.Fatalf("节点 %s 未追赶到 main2=2, 读到 %q", id, v)
		}
	}
	t.Logf("日志冲突覆盖测试通过")
}
