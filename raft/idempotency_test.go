package raft

import "testing"

// 客户端超时重试（相同 ClientID+Seq）不得产生重复条目，结果必须一致。
func TestIdempotentClientRetry(t *testing.T) {
	c := newTestCluster(t, "a", "b", "c")
	leader := c.WaitLeader(100)
	if leader == nil {
		t.Fatal("no leader")
	}
	req := ClientWrite{ClientID: "cli1", Seq: 1, Command: []byte("set:k:v")}
	r1 := leader.Write(req)
	if !r1.Committed {
		t.Fatalf("first write not committed: %+v", r1)
	}
	// 模拟客户端超时后重试同一请求多次
	for i := 0; i < 3; i++ {
		r := leader.Write(req)
		if !r.OK || !r.Committed {
			t.Fatalf("retry %d failed: %+v", i, r)
		}
	}
	// 乱序旧请求（更小 Seq）也必须被去重
	r := leader.Write(ClientWrite{ClientID: "cli1", Seq: 1, Command: []byte("set:k:stale")})
	if !r.Committed {
		t.Fatalf("stale retry should still report committed: %+v", r)
	}
	c.Ticks(5)
	for _, id := range []string{"a", "b", "c"} {
		kv := c.Node(id).AppliedKV()
		if kv["k"] != "v" {
			t.Fatalf("node %s got %q, want v (duplicate applied?)", id, kv["k"])
		}
		_, _, _, last := c.Node(id).Status()
		// 日志应为 no-op + 1 条客户端写入 = 2 条
		if last != 2 {
			t.Fatalf("node %s lastIndex=%d, want 2 (duplicate entry appended?)", id, last)
		}
	}
}

// 节点宕机重启后：持久化状态恢复，已提交数据不丢，能重新加入集群。
func TestCrashRestartRecovery(t *testing.T) {
	c := newTestCluster(t, "a", "b", "c")
	leader := c.WaitLeader(100)
	if leader == nil {
		t.Fatal("no leader")
	}
	for i := 1; i <= 5; i++ {
		w := leader.Write(ClientWrite{ClientID: "cli1", Seq: int64(i), Command: []byte("set:k" + itoa(i) + ":v")})
		if !w.Committed {
			t.Fatalf("write %d not committed", i)
		}
	}
	c.Ticks(5)

	// 杀掉一个跟随者再重启
	victim := "a"
	if leader.ID() == "a" {
		victim = "b"
	}
	c.StopNode(victim)
	if err := c.RestartNode(victim); err != nil {
		t.Fatalf("restart: %v", err)
	}
	// 重启后先写入新值，等心跳让重启节点追平
	w := leader.Write(ClientWrite{ClientID: "cli1", Seq: 6, Command: []byte("set:k6:v")})
	if !w.Committed {
		t.Fatal("write after restart not committed")
	}
	c.Ticks(20)
	kv := c.Node(victim).AppliedKV()
	for i := 1; i <= 6; i++ {
		if kv["k"+itoa(i)] != "v" {
			t.Fatalf("restarted node missing k%d: %v", i, kv)
		}
	}
	role, term, commit, _ := c.Node(victim).Status()
	t.Logf("restarted node role=%s term=%d commit=%d", role, term, commit)
}

func itoa(i int) string {
	return string(rune('0' + i))
}
