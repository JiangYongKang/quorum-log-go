package raft

import (
	"testing"
	"time"
)

// TestIdempotentRetry 验证：同一 ClientID+Seq 的超时重试不会产生重复条目，
// 状态机只应用一次。
func TestIdempotentRetry(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	leader := c.WaitLeader(5 * time.Second)
	if leader == nil {
		t.Fatal("未选出领导")
	}
	cmd := Command{Op: "set", Key: "x", Value: "1", ClientID: "cli", Seq: 1}

	idx1, err := leader.Propose(cmd)
	if err != nil {
		t.Fatalf("首次写入失败: %v", err)
	}
	// 模拟客户端超时重试：同一命令再发多次（含并发）。
	results := make(chan uint64, 4)
	for i := 0; i < 4; i++ {
		go func() {
			idx, err := leader.Propose(cmd)
			if err != nil {
				t.Errorf("重试写入失败: %v", err)
				return
			}
			results <- idx
		}()
	}
	for i := 0; i < 4; i++ {
		select {
		case idx := <-results:
			if idx != idx1 {
				t.Fatalf("重试返回了不同 index: %d != %d（产生了重复条目）", idx, idx1)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("重试超时")
		}
	}

	// 日志中该命令只应出现一次。
	if !c.WaitCommitAll(idx1, 5*time.Second) {
		t.Fatal("提交未收敛")
	}
	for _, id := range c.IDs {
		n := c.Nodes[id]
		count := 0
		for _, e := range n.DumpLog() {
			if e.Command.ClientID == "cli" && e.Command.Seq == 1 {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("节点 %s 日志中 cli/1 出现 %d 次，期望 1 次", id, count)
		}
		if v, _ := n.ReadCommitted("x"); v != "1" {
			t.Fatalf("节点 %s 读到 x=%q，期望 1", id, v)
		}
	}
	t.Logf("幂等重试通过: index=%d 全集群唯一条目", idx1)
}
