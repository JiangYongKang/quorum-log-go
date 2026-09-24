package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSnapshotCatchUp 验证：
//  1. 日志超过阈值后触发快照压缩（snapshot.json 落盘，WAL 变短）；
//  2. 落后节点通过 InstallSnapshot + 增量日志追赶；
//  3. 追赶完成后状态与未压缩节点完全一致。
func TestSnapshotCatchUp(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.SnapshotThreshold = 5 })
	leader := c.WaitLeader(5 * time.Second)
	if leader == nil {
		t.Fatal("未选出领导")
	}

	// 隔离一个跟随者，使其落后。
	lagging := "n3"
	if leader.ID() == lagging {
		lagging = "n2"
	}
	c.Partition(lagging)

	// 写入足够多的条目触发多数派快照。
	const total = 20
	for i := 1; i <= total; i++ {
		mustPropose(t, c, Command{
			Op: "set", Key: fmt.Sprintf("key%02d", i), Value: fmt.Sprintf("val%02d", i),
			ClientID: "c", Seq: uint64(i),
		})
	}

	// 确认多数派已生成快照且 WAL 被压缩。
	for _, id := range c.IDs {
		if id == lagging {
			continue
		}
		snapPath := filepath.Join(c.dirs[id], "snapshot.json")
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, err := os.Stat(snapPath); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("节点 %s 未生成快照", id)
			}
			time.Sleep(20 * time.Millisecond)
		}
		n := c.Nodes[id]
		if got := len(n.DumpLog()); got > 15 {
			t.Fatalf("节点 %s 快照后 WAL 仍有 %d 条，压缩未生效", id, got)
		}
		t.Logf("节点 %s 快照完成, 剩余日志 %d 条", id, len(n.DumpLog()))
	}

	// 恢复分区：落后节点应通过快照+增量追赶。
	c.Heal()
	if !c.WaitCommit(lagging, total+1, 10*time.Second) {
		s := c.Nodes[lagging].Status()
		t.Fatalf("落后节点未追赶到 commit=%d, 当前 commit=%d last=%d", total+1, s.CommitIndex, s.LastIndex)
	}

	// 状态一致性：逐键比对三个节点。
	for i := 1; i <= total; i++ {
		k := fmt.Sprintf("key%02d", i)
		want := fmt.Sprintf("val%02d", i)
		for _, id := range c.IDs {
			if v, _ := c.Nodes[id].ReadCommitted(k); v != want {
				t.Fatalf("节点 %s 键 %s=%q, 期望 %q", id, k, v, want)
			}
		}
	}
	t.Logf("快照追赶测试通过: 落后节点状态与多数派完全一致")
}

// TestSnapshotCatchUpInterrupted 验证：追赶过程中宕机重启不会留下部分应用的状态，
// 重启后重新追赶，最终状态一致。
func TestSnapshotCatchUpInterrupted(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.SnapshotThreshold = 5 })
	leader := c.WaitLeader(5 * time.Second)
	if leader == nil {
		t.Fatal("未选出领导")
	}
	lagging := "n3"
	if leader.ID() == lagging {
		lagging = "n2"
	}
	c.Partition(lagging)
	const total = 15
	for i := 1; i <= total; i++ {
		mustPropose(t, c, Command{
			Op: "set", Key: fmt.Sprintf("k%02d", i), Value: fmt.Sprintf("v%02d", i),
			ClientID: "c", Seq: uint64(i),
		})
	}
	// 等多数派完成快照。
	time.Sleep(300 * time.Millisecond)

	// 恢复分区后立刻宕机落后节点（追赶/快照安装可能进行到一半）。
	c.Heal()
	time.Sleep(30 * time.Millisecond)
	c.Crash(lagging)
	c.Partition(lagging) // 保持隔离，确保它停在中间态
	time.Sleep(100 * time.Millisecond)

	// 重启并恢复网络：应重新追赶到达一致状态。
	if err := c.Restart(lagging); err != nil {
		t.Fatalf("重启失败: %v", err)
	}
	c.Heal()
	if !c.WaitCommit(lagging, total+1, 10*time.Second) {
		s := c.Nodes[lagging].Status()
		t.Fatalf("中断恢复后未追赶到 commit=%d, 当前 commit=%d", total+1, s.CommitIndex)
	}
	for i := 1; i <= total; i++ {
		k := fmt.Sprintf("k%02d", i)
		want := fmt.Sprintf("v%02d", i)
		if v, _ := c.Nodes[lagging].ReadCommitted(k); v != want {
			t.Fatalf("中断恢复后节点 %s 键 %s=%q, 期望 %q", lagging, k, v, want)
		}
	}
	t.Logf("追赶中断恢复测试通过")
}
