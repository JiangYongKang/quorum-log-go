package raft

import (
	"testing"
	"time"
)

// TestPartitionConvergesToSingleLeader 验证：
//  1. 隔离老领导后，多数派收敛出唯一新领导；
//  2. 被隔离的老领导无法提交新条目（任期推进但提交位点不动）；
//  3. 分区恢复后全集群重新收敛到唯一领导、提交位点一致。
func TestPartitionConvergesToSingleLeader(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	old := c.WaitLeader(5 * time.Second)
	if old == nil {
		t.Fatal("未选出领导")
	}
	oldID := old.ID()
	mustPropose(t, c, Command{Op: "set", Key: "a", Value: "1", ClientID: "c1", Seq: 1})

	// 隔离老领导。
	c.Partition(oldID)
	t.Logf("已隔离老领导 %s", oldID)

	// 多数派应选出新领导（此时老领导仍自认为领导，属预期）。
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
		t.Fatal("分区后多数派未选出新领导")
	}
	t.Logf("新领导=%s term=%d", newLeader.ID(), newLeader.Status().Term)

	// 新领导可提交；老领导（被隔离）提出的写入无法提交。
	idx := mustPropose(t, c, Command{Op: "set", Key: "b", Value: "2", ClientID: "c1", Seq: 2})
	// 直接向被隔离的老领导发写入：应因无法达成多数而挂起/失败。
	go func() {
		old.Propose(Command{Op: "set", Key: "ghost", Value: "x", ClientID: "c1", Seq: 99})
	}()
	time.Sleep(500 * time.Millisecond)
	if v, _ := old.ReadCommitted("ghost"); v != "" {
		t.Fatalf("被隔离的老领导提交了未获多数确认的条目 ghost=%q", v)
	}
	if v, _ := old.ReadCommitted("b"); v != "" {
		t.Fatalf("被隔离的老领导不应看到分区期间的提交 b=%q", v)
	}

	// 恢复分区，集群重新收敛。
	c.Heal()
	if !c.WaitCommitAll(idx, 5*time.Second) {
		t.Fatal("分区恢复后提交位点未收敛")
	}
	// 任一时刻唯一领导。
	seen := map[string]bool{}
	for i := 0; i < 20; i++ {
		cnt := 0
		for _, n := range c.Nodes {
			if n.Status().Role == Leader {
				cnt++
			}
		}
		if cnt > 1 {
			t.Fatalf("检测到 %d 个领导并存", cnt)
		}
		seen["ok"] = true
		time.Sleep(20 * time.Millisecond)
	}
	// 老领导追赶后应能看到分区期间的提交。
	if v, _ := c.Nodes[oldID].ReadCommitted("b"); v != "2" {
		t.Fatalf("老领导恢复后未追赶到 b=2, 读到 %q", v)
	}
	// ghost 条目未获多数，不得呈现为已提交。
	if v, _ := c.Nodes[oldID].ReadCommitted("ghost"); v != "" {
		t.Fatalf("未提交条目 ghost 被错误呈现为已提交: %q", v)
	}
	t.Logf("分区收敛测试通过")
}
