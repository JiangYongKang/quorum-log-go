package raft

import (
	"fmt"
	"log"
)

// Cluster 是单机多节点骨架：共享一个模拟网络，节点各自持有独立数据目录。
type Cluster struct {
	net    *Network
	nodes  map[string]*Node
	cfgs   map[string]Config
	logger *log.Logger
}

// NewCluster 按 ids 创建集群；每个节点使用 dirs 中同名子目录持久化。
func NewCluster(ids []string, dir string, logger *log.Logger) (*Cluster, error) {
	c := &Cluster{
		net:    NewNetwork(),
		nodes:  map[string]*Node{},
		cfgs:   map[string]Config{},
		logger: logger,
	}
	for _, id := range ids {
		cfg := Config{
			ID:                id,
			Peers:             ids,
			DataDir:           fmt.Sprintf("%s/%s", dir, id),
			ElectionTimeout:   10,
			HeartbeatTick:     2,
			SnapshotThreshold: 20,
		}
		node, err := NewNode(cfg, c.net, logger)
		if err != nil {
			return nil, err
		}
		c.nodes[id] = node
		c.cfgs[id] = cfg
	}
	return c, nil
}

// Start 启动全部节点。
func (c *Cluster) Start() {
	for _, n := range c.nodes {
		n.Start()
	}
}

// Tick 推进所有存活节点一个时钟单位（确定性）。
func (c *Cluster) Tick() {
	for _, n := range c.nodes {
		n.Tick()
	}
}

// Ticks 推进 k 个时钟单位。
func (c *Cluster) Ticks(k int) {
	for i := 0; i < k; i++ {
		c.Tick()
	}
}

// StopNode 停掉一个节点（模拟宕机，数据保留）。
func (c *Cluster) StopNode(id string) {
	if n, ok := c.nodes[id]; ok {
		n.Stop()
	}
}

// KillNode 强杀一个节点：立即从网络摘除并停止时钟，不做任何收尾，
// 等价于进程被 kill/掉电。持久化数据保留在磁盘（含可能的中途残留），
// 由 RestartNode 时的崩溃恢复流程处理。
func (c *Cluster) KillNode(id string) {
	c.StopNode(id)
}

// RestartNode 重启节点：从磁盘重建状态并重新接入网络。
func (c *Cluster) RestartNode(id string) error {
	cfg := c.cfgs[id]
	node, err := NewNode(cfg, c.net, c.logger)
	if err != nil {
		return err
	}
	c.nodes[id] = node
	node.Start()
	return nil
}

// Node 取节点。
func (c *Cluster) Node(id string) *Node { return c.nodes[id] }

// Net 取网络。
func (c *Cluster) Net() *Network { return c.net }

// Leader 返回当前唯一领导；不存在或存在多个时返回 nil。
func (c *Cluster) Leader() *Node {
	var leader *Node
	for _, n := range c.nodes {
		role, _, _, _ := n.Status()
		if role == Leader {
			if leader != nil {
				return nil
			}
			leader = n
		}
	}
	return leader
}

// WaitLeader 最多推进 maxTicks 个时钟，直到出现唯一领导。
func (c *Cluster) WaitLeader(maxTicks int) *Node {
	for i := 0; i < maxTicks; i++ {
		if l := c.Leader(); l != nil {
			return l
		}
		c.Tick()
	}
	return c.Leader()
}
