package raft

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// newTestCluster 构建 n 节点测试集群，日志输出到测试日志（含任期/提交位点/落盘流水）。
func newTestCluster(t *testing.T, n int, tweak func(*Config)) *Cluster {
	t.Helper()
	ids := make([]string, n)
	dirs := make(map[string]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("n%d", i+1)
		dirs[ids[i]] = filepath.Join(t.TempDir(), ids[i])
	}
	cfgFn := func(id string, peers []string, dir string) Config {
		cfg := Config{
			ID:                id,
			Peers:             peers,
			Dir:               dir,
			ElectionTickMin:   15,
			ElectionTickMax:   30,
			HeartbeatTick:     3,
			TickIntervalMs:    10,
			SnapshotThreshold: 50,
		}
		if tweak != nil {
			tweak(&cfg)
		}
		return cfg
	}
	logf := func(format string, args ...any) { t.Logf(format, args...) }
	c := NewCluster(ids, dirs, cfgFn, logf)
	if err := c.StartAll(); err != nil {
		t.Fatalf("启动集群失败: %v", err)
	}
	t.Cleanup(c.StopAll)
	return c
}

// mustPropose 向当前领导提交写入，领导变化或分区期间自动重试。
// 每次尝试带超时，避免被隔离领导的挂起请求阻塞。
func mustPropose(t *testing.T, c *Cluster, cmd Command) uint64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.Nodes {
			if n.Status().Role != Leader {
				continue
			}
			type res struct {
				idx uint64
				err error
			}
			ch := make(chan res, 1)
			go func(n *Node) {
				idx, err := n.Propose(cmd)
				ch <- res{idx, err}
			}(n)
			select {
			case r := <-ch:
				if r.err == nil {
					return r.idx
				}
			case <-time.After(time.Second):
				// 该领导可能无法达成多数，换下一个。
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("propose 超时: %+v", cmd)
	return 0
}

func TestElectionAndReplication(t *testing.T) {
	c := newTestCluster(t, 3, nil)

	leader := c.WaitLeader(5 * time.Second)
	if leader == nil {
		t.Fatal("未选出领导")
	}
	st := leader.Status()
	t.Logf("领导=%s term=%d", leader.ID(), st.Term)

	// 写入若干条，验证多数提交与读可见。
	for i := 1; i <= 5; i++ {
		mustPropose(t, c, Command{
			Op: "set", Key: fmt.Sprintf("k%d", i), Value: fmt.Sprintf("v%d", i),
			ClientID: "cli", Seq: uint64(i),
		})
	}
	if !c.WaitCommitAll(6, 5*time.Second) { // 1 条 noop + 5 条写入
		for _, id := range c.IDs {
			s := c.Nodes[id].Status()
			t.Logf("节点 %s: role=%v term=%d commit=%d last=%d", id, s.Role, s.Term, s.CommitIndex, s.LastIndex)
		}
		t.Fatal("提交位点未收敛")
	}
	for _, id := range c.IDs {
		v, ok := c.Nodes[id].ReadCommitted("k3")
		if !ok || v != "v3" {
			t.Fatalf("节点 %s 读到 k3=%q ok=%v, 期望 v3", id, v, ok)
		}
	}
	t.Logf("正常路径通过: 全部节点已提交并可见")
}
