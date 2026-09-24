package raft

import (
	"log"
	"os"
	"testing"
)

func newTestLogger(t *testing.T) *log.Logger {
	t.Helper()
	return log.New(testWriter{t}, "", 0)
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", string(p))
	return len(p), nil
}

func newTestCluster(t *testing.T, ids ...string) *Cluster {
	t.Helper()
	dir := t.TempDir()
	c, err := NewCluster(ids, dir, newTestLogger(t))
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	c.Start()
	return c
}

func TestElectionAndBasicReplication(t *testing.T) {
	c := newTestCluster(t, "a", "b", "c")
	leader := c.WaitLeader(100)
	if leader == nil {
		t.Fatal("no leader elected")
	}
	t.Logf("leader=%s", leader.ID())

	reply := leader.Write(ClientWrite{ClientID: "cli1", Seq: 1, Command: []byte("set:x:1")})
	if !reply.OK || !reply.Committed {
		t.Fatalf("write not committed: %+v", reply)
	}
	c.Ticks(5) // 心跳把提交位点同步给跟随者

	for _, id := range []string{"a", "b", "c"} {
		kv := c.Node(id).AppliedKV()
		if kv["x"] != "1" {
			t.Fatalf("node %s missing applied value: %v", id, kv)
		}
		_, term, commit, _ := c.Node(id).Status()
		t.Logf("node %s term=%d commit=%d", id, term, commit)
	}
	if os.Getenv("VERBOSE") != "" {
		t.Log("done")
	}
}
