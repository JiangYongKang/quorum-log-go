package raft

import "testing"

// 网络分区后多数派一侧必须收敛出唯一领导；愈合后全集群回到唯一领导，
// 且同一任期内不存在两个都能提交日志的领导。
func TestPartitionConvergesToSingleLeader(t *testing.T) {
	c := newTestCluster(t, "a", "b", "c", "d", "e")
	leader := c.WaitLeader(200)
	if leader == nil {
		t.Fatal("no leader elected")
	}
	oldLeaderID := leader.ID()
	_, term0, _, _ := leader.Status()

	// 把旧领导和另一个节点切到少数派分区
	others := []string{}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		if id != oldLeaderID {
			others = append(others, id)
		}
	}
	minority := []string{oldLeaderID, others[0]}
	majority := others[1:]
	c.Net().Partition(minority, majority)

	// 少数派无法提交：旧领导写入不应提交
	r := leader.Write(ClientWrite{ClientID: "cli1", Seq: 1, Command: []byte("set:k:minority")})
	if r.Committed {
		t.Fatal("minority leader must not commit")
	}

	// 多数派选出新领导
	var newLeader *Node
	for i := 0; i < 300; i++ {
		c.Tick()
		for _, id := range majority {
			if role, _, _, _ := c.Node(id).Status(); role == Leader {
				newLeader = c.Node(id)
			}
		}
		if newLeader != nil {
			break
		}
	}
	if newLeader == nil {
		t.Fatal("majority side did not elect a leader")
	}
	_, term1, _, _ := newLeader.Status()
	if term1 <= term0 {
		t.Fatalf("new leader term %d should exceed %d", term1, term0)
	}

	// 新领导可提交
	r = newLeader.Write(ClientWrite{ClientID: "cli1", Seq: 2, Command: []byte("set:k:majority")})
	if !r.Committed {
		t.Fatal("majority leader must commit")
	}

	// 愈合后集群收敛到唯一领导
	c.Net().Heal()
	c.Ticks(200)
	leaders := 0
	var finalTerm int
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		role, term, _, _ := c.Node(id).Status()
		if role == Leader {
			leaders++
			finalTerm = term
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly 1 leader after heal, got %d", leaders)
	}
	// 所有节点已应用 majority 写入，少数派的未提交条目不得呈现为已提交
	c.Ticks(10)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		kv := c.Node(id).AppliedKV()
		if kv["k"] != "majority" {
			t.Fatalf("node %s applied %q, want majority value", id, kv["k"])
		}
	}
	t.Logf("converged: single leader term=%d", finalTerm)
}
