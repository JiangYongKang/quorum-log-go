package raft

import (
	"fmt"
	"testing"
)

// 落后节点在领导完成快照压缩后，应通过 InstallSnapshot + 增量日志追平，
// 且追平后的状态与未压缩节点完全一致。
func TestSnapshotCatchUp(t *testing.T) {
	c := newTestCluster(t, "a", "b", "c")
	leader := c.WaitLeader(100)
	if leader == nil {
		t.Fatal("no leader")
	}
	// 隔离跟随者 c，使其错过全部写入
	c.Net().Disconnect(leader.ID(), "c")
	c.Net().Disconnect("c", "a")
	c.Net().Disconnect("c", "b")

	// 写入超过快照阈值（20 条）的数据，触发领导侧快照
	for i := 1; i <= 30; i++ {
		r := leader.Write(ClientWrite{ClientID: "cli1", Seq: int64(i),
			Command: []byte(fmt.Sprintf("set:key%02d:val%02d", i, i))})
		if !r.Committed {
			t.Fatalf("write %d not committed", i)
		}
	}
	c.Ticks(5)
	_, _, lcommit, llast := leader.Status()
	t.Logf("leader commit=%d last=%d (snapshot expected to compact prefix)", lcommit, llast)

	// 恢复网络，c 应通过快照追平
	c.Net().Heal()
	c.Ticks(50)

	want := leader.AppliedKV()
	got := c.Node("c").AppliedKV()
	if len(got) != len(want) {
		t.Fatalf("caught-up node has %d keys, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("key %s: got %q want %q", k, got[k], v)
		}
	}
	_, _, ccommit, _ := c.Node("c").Status()
	if ccommit != lcommit {
		t.Fatalf("follower commit=%d, leader commit=%d", ccommit, lcommit)
	}
	t.Logf("node c caught up via snapshot: commit=%d keys=%d", ccommit, len(got))
}

// 追赶/恢复中途中断（宕机）不得留下被部分应用的状态：
// 重启后要么保持旧快照状态，要么完整追平，最终必须与领导一致。
func TestInterruptDuringCatchUp(t *testing.T) {
	c := newTestCluster(t, "a", "b", "c")
	leader := c.WaitLeader(100)
	if leader == nil {
		t.Fatal("no leader")
	}
	c.Net().Disconnect(leader.ID(), "c")
	c.Net().Disconnect("c", "a")
	c.Net().Disconnect("c", "b")
	for i := 1; i <= 30; i++ {
		leader.Write(ClientWrite{ClientID: "cli1", Seq: int64(i),
			Command: []byte(fmt.Sprintf("set:key%02d:val%02d", i, i))})
	}
	c.Ticks(5)

	// c 宕机，再恢复网络，重启 c —— 从磁盘恢复后通过快照+增量追平
	c.StopNode("c")
	c.Net().Heal()
	if err := c.RestartNode("c"); err != nil {
		t.Fatalf("restart: %v", err)
	}
	c.Ticks(80)

	want := leader.AppliedKV()
	got := c.Node("c").AppliedKV()
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("after restart key %s: got %q want %q", k, got[k], v)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("after restart got %d keys want %d", len(got), len(want))
	}
	t.Logf("restarted node fully recovered, %d keys", len(got))
}
