package raft

import "testing"

// 分区期间少数派领导产生的未提交条目，必须被新领导的日志覆盖，
// 且任何时刻不得对外呈现为已提交。
func TestLogConflictOverwrite(t *testing.T) {
	c := newTestCluster(t, "a", "b", "c", "d", "e")
	leader := c.WaitLeader(200)
	if leader == nil {
		t.Fatal("no leader")
	}
	oldID := leader.ID()
	// 先提交一条基准值
	if r := leader.Write(ClientWrite{ClientID: "cli1", Seq: 1, Command: []byte("set:base:ok")}); !r.Committed {
		t.Fatal("base write not committed")
	}
	c.Ticks(5)

	// 分区：旧领导 + 1 个跟随者 vs 3 个节点
	others := []string{}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		if id != oldID {
			others = append(others, id)
		}
	}
	c.Net().Partition([]string{oldID, others[0]}, others[1:])

	// 旧领导在少数派写入 3 条，无法达到多数派，不得提交
	for i := int64(2); i <= 4; i++ {
		r := leader.Write(ClientWrite{ClientID: "cli1", Seq: i, Command: []byte("set:uncommitted:x")})
		if r.Committed {
			t.Fatalf("minority write seq=%d must not commit", i)
		}
	}

	// 多数派选新领导并提交不同的条目
	var newLeader *Node
	for i := 0; i < 300 && newLeader == nil; i++ {
		c.Tick()
		for _, id := range others[1:] {
			if role, _, _, _ := c.Node(id).Status(); role == Leader {
				newLeader = c.Node(id)
			}
		}
	}
	if newLeader == nil {
		t.Fatal("no new leader in majority")
	}
	for i := int64(5); i <= 7; i++ {
		if r := newLeader.Write(ClientWrite{ClientID: "cli1", Seq: i, Command: []byte("set:committed:y")}); !r.Committed {
			t.Fatalf("majority write seq=%d not committed", i)
		}
	}

	// 愈合：旧领导的未提交条目被覆盖，全集群状态一致
	c.Net().Heal()
	c.Ticks(200)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		kv := c.Node(id).AppliedKV()
		if _, bad := kv["uncommitted"]; bad {
			t.Fatalf("node %s shows overwritten uncommitted entry as applied", id)
		}
		if kv["committed"] != "y" || kv["base"] != "ok" {
			t.Fatalf("node %s state %v, want committed entries", id, kv)
		}
	}
	// 全集群日志末端一致
	_, _, commit0, last0 := c.Node("a").Status()
	for _, id := range []string{"b", "c", "d", "e"} {
		_, _, commit, last := c.Node(id).Status()
		if commit != commit0 || last != last0 {
			t.Fatalf("node %s commit=%d last=%d, want commit=%d last=%d", id, commit, last, commit0, last0)
		}
	}
	t.Logf("logs converged at commit=%d last=%d", commit0, last0)
}
