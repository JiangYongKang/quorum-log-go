package raft

import "sync"

// Network 是单机模拟网络：节点间消息投递与分区都在这里控制。
// RPC 语义为同步调用；断开的链路直接返回不可达。
type Network struct {
	mu      sync.RWMutex
	nodes   map[string]*Node
	blocked map[[2]string]bool // 单向断边 [from,to]
}

// NewNetwork 创建模拟网络。
func NewNetwork() *Network {
	return &Network{nodes: map[string]*Node{}, blocked: map[[2]string]bool{}}
}

// Register 将节点接入网络。
func (n *Network) Register(id string, node *Node) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = node
}

// Unregister 将节点从网络摘除（模拟宕机）。
func (n *Network) Unregister(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.nodes, id)
}

// Connect 恢复两个节点间双向通信。
func (n *Network) Connect(a, b string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.blocked, [2]string{a, b})
	delete(n.blocked, [2]string{b, a})
}

// Disconnect 切断两个节点间双向通信。
func (n *Network) Disconnect(a, b string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked[[2]string{a, b}] = true
	n.blocked[[2]string{b, a}] = true
}

// Partition 将集群切成两组，组内互通、组间断开。
func (n *Network) Partition(groupA, groupB []string) {
	for _, a := range groupA {
		for _, b := range groupB {
			n.Disconnect(a, b)
		}
	}
}

// Heal 恢复全连通。
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = map[[2]string]bool{}
}

// reachable 检查 from->to 是否可达并取出目标节点。
func (n *Network) reachable(from, to string) (*Node, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.blocked[[2]string{from, to}] {
		return nil, false
	}
	node, ok := n.nodes[to]
	return node, ok
}

// SendVote 投递投票请求并同步等待响应（模拟 RPC）。
func (n *Network) SendVote(from, to string, req RequestVote) (RequestVoteReply, bool) {
	node, ok := n.reachable(from, to)
	if !ok {
		return RequestVoteReply{}, false
	}
	return node.HandleVote(req), true
}

// SendAppend 投递日志复制请求。
func (n *Network) SendAppend(from, to string, req AppendEntries) (AppendEntriesReply, bool) {
	node, ok := n.reachable(from, to)
	if !ok {
		return AppendEntriesReply{}, false
	}
	return node.HandleAppend(req), true
}

// SendSnapshot 投递快照安装请求。
func (n *Network) SendSnapshot(from, to string, req InstallSnapshot) (InstallSnapshotReply, bool) {
	node, ok := n.reachable(from, to)
	if !ok {
		return InstallSnapshotReply{}, false
	}
	return node.HandleSnapshot(req), true
}
