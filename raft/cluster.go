package raft

import (
	"fmt"
	"time"
)

// Cluster 是单机多节点集群封装：节点跑在本进程的 goroutine 中，
// 通信走 MemTransport，持久化落在各自目录，可模拟宕机、重启与分区。
type Cluster struct {
	IDs       []string
	Nodes     map[string]*Node
	Transport *MemTransport
	dirs      map[string]string
	cfgFn     func(id string, peers []string, dir string) Config
	logf      func(string, ...any)
}

// NewCluster 创建集群（暂不启动）。dirs 为每个节点提供独立持久化目录。
func NewCluster(ids []string, dirs map[string]string,
	cfgFn func(id string, peers []string, dir string) Config,
	logf func(string, ...any)) *Cluster {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Cluster{
		IDs:       ids,
		Nodes:     make(map[string]*Node),
		Transport: NewMemTransport(),
		dirs:      dirs,
		cfgFn:     cfgFn,
		logf:      logf,
	}
}

// StartAll 启动全部节点。
func (c *Cluster) StartAll() error {
	for _, id := range c.IDs {
		if err := c.startNode(id); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) startNode(id string) error {
	cfg := c.cfgFn(id, c.IDs, c.dirs[id])
	n, err := NewNode(cfg, c.Transport, c.logf)
	if err != nil {
		return fmt.Errorf("启动节点 %s: %w", id, err)
	}
	n.Start()
	c.Nodes[id] = n
	return nil
}

// Crash 停掉节点（保留磁盘状态，模拟宕机）。
func (c *Cluster) Crash(id string) {
	if n, ok := c.Nodes[id]; ok {
		n.Stop()
		delete(c.Nodes, id)
		c.logf("[cluster] 节点 %s 宕机", id)
	}
}

// Restart 用原目录重启节点（模拟宕机恢复）。
func (c *Cluster) Restart(id string) error {
	c.logf("[cluster] 节点 %s 重启", id)
	return c.startNode(id)
}

// StopAll 停止全部节点。
func (c *Cluster) StopAll() {
	for id := range c.Nodes {
		n := c.Nodes[id]
		n.Stop()
		delete(c.Nodes, id)
	}
}

// Partition 隔离指定节点（与其余节点双向断连）。
func (c *Cluster) Partition(ids ...string) {
	for _, id := range ids {
		c.Transport.Cut(id)
	}
	c.logf("[cluster] 分区: 隔离 %v", ids)
}

// Heal 恢复全部网络。
func (c *Cluster) Heal() {
	c.Transport.HealAll()
	c.logf("[cluster] 分区恢复")
}

// Leader 返回当前唯一领导；没有或多个时返回 nil。
func (c *Cluster) Leader() *Node {
	var leader *Node
	for _, n := range c.Nodes {
		if n.Status().Role == Leader {
			if leader != nil {
				return nil
			}
			leader = n
		}
	}
	return leader
}

// WaitLeader 在超时时间内等待集群收敛出唯一领导。
func (c *Cluster) WaitLeader(timeout time.Duration) *Node {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l != nil {
			return l
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// WaitCommit 等待节点提交位点到达 index。
func (c *Cluster) WaitCommit(id string, index uint64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n, ok := c.Nodes[id]; ok && n.Status().CommitIndex >= index {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// WaitCommitAll 等待所有存活节点提交位点到达 index。
func (c *Cluster) WaitCommitAll(index uint64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, n := range c.Nodes {
			if n.Status().CommitIndex < index {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}
