package raft

import "sync"

// Transport 抽象节点间通信。内存实现可模拟分区、单向隔离与丢包。
type Transport interface {
	Send(m Message)
	Cut(from string)
	Heal(from string)
	HealAll()
}

// MemTransport 进程内消息总线：Send 为非阻塞投递到目标节点收件箱。
// 被 Cut 的节点既发不出也收不到（对称分区）。
type MemTransport struct {
	mu      sync.Mutex
	inboxes map[string]chan Message
	cut     map[string]bool
	dropped uint64 // 统计：因分区被丢弃的消息数
}

func NewMemTransport() *MemTransport {
	return &MemTransport{
		inboxes: make(map[string]chan Message),
		cut:     make(map[string]bool),
	}
}

// Register 为节点注册收件箱，返回接收通道。
func (t *MemTransport) Register(id string) <-chan Message {
	t.mu.Lock()
	defer t.mu.Unlock()
	ch := make(chan Message, 4096)
	t.inboxes[id] = ch
	return ch
}

// Unregister 移除节点收件箱（模拟宕机：消息直接丢弃）。
func (t *MemTransport) Unregister(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.inboxes, id)
}

// Send 非阻塞投递；发送方或接收方被隔离、接收方宕机时消息丢弃。
func (t *MemTransport) Send(m Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cut[m.From] || t.cut[m.To] {
		t.dropped++
		return
	}
	ch, ok := t.inboxes[m.To]
	if !ok {
		t.dropped++ // 目标宕机
		return
	}
	select {
	case ch <- m:
	default:
		t.dropped++ // 收件箱满，按丢包处理
	}
}

// Cut 切断 from 与所有其他节点的双向通信。
func (t *MemTransport) Cut(from string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cut[from] = true
}

// Heal 恢复 from 的通信。
func (t *MemTransport) Heal(from string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.cut, from)
}

// HealAll 恢复全部通信。
func (t *MemTransport) HealAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cut = make(map[string]bool)
}

// Dropped 返回被丢弃的消息总数（测试观测用）。
func (t *MemTransport) Dropped() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dropped
}
