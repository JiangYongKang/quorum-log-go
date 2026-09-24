package raft

// 快照与日志压缩、落后节点追赶。
// 不变式：
//   - 快照只覆盖已提交并已应用的前缀；
//   - 快照落盘与 WAL 截断分两步原子写，宕机恢复后状态等价于"快照未发生"或"快照已完成"，
//     绝不出现部分应用；
//   - 追赶完成后，节点状态机与"从未压缩、逐条重放"完全一致。

// maybeSnapshot 已应用且未压缩的日志超过阈值时触发压缩。
func (n *Node) maybeSnapshot() {
	if n.cfg.SnapshotThreshold <= 0 {
		return
	}
	if uint64(len(n.entries)) < uint64(n.cfg.SnapshotThreshold) {
		return
	}
	if n.lastApplied <= n.snapIndex {
		return
	}
	n.takeSnapshot(n.lastApplied)
}

// takeSnapshot 对 [snapIndex+1, upto] 的已应用前缀生成快照并压缩 WAL。
func (n *Node) takeSnapshot(upto uint64) {
	if upto <= n.snapIndex {
		return
	}
	data := make(map[string]string, len(n.kv))
	for k, v := range n.kv {
		data[k] = v
	}
	seq := make(map[string]uint64, len(n.clientSeq))
	for k, v := range n.clientSeq {
		seq[k] = v
	}
	meta := &SnapshotMeta{
		LastIncludedIndex: upto,
		LastIncludedTerm:  n.termAt(upto),
		Data:              data,
		ClientSeq:         seq,
	}
	remaining := n.sliceFrom(upto + 1)
	if err := n.st.saveSnapshot(meta, remaining); err != nil {
		n.logf("快照落盘失败: %v", err)
		return
	}
	n.lastSnap = meta
	n.snapIndex = upto
	n.snapTerm = meta.LastIncludedTerm
	n.entries = remaining
	n.logf("日志压缩完成: snapIndex=%d snapTerm=%d 剩余日志=%d条", n.snapIndex, n.snapTerm, len(n.entries))
}

// sendSnapshotTo 向落后太多的跟随者发送快照。
func (n *Node) sendSnapshotTo(peer string) {
	if n.lastSnap == nil {
		return
	}
	n.logf("向 %s 发送快照: lastIncludedIndex=%d (其 nextIndex=%d 已被压缩)",
		peer, n.lastSnap.LastIncludedIndex, n.nextIndex[peer])
	n.tr.Send(Message{From: n.cfg.ID, To: peer, Kind: "snap-req",
		SnapReq: &InstallSnapshotReq{
			Term:              n.term,
			LeaderID:          n.cfg.ID,
			LastIncludedIndex: n.lastSnap.LastIncludedIndex,
			LastIncludedTerm:  n.lastSnap.LastIncludedTerm,
			Data:              n.lastSnap.Data,
			ClientSeq:         n.lastSnap.ClientSeq,
		}})
}

// onInstallSnapshotReq 跟随者接收快照：原子落盘后整体替换状态，
// 中途中断（宕机）时磁盘上仍是旧快照+旧 WAL，恢复后重收即可。
func (n *Node) onInstallSnapshotReq(req InstallSnapshotReq) {
	if req.Term < n.term {
		n.tr.Send(Message{From: n.cfg.ID, To: req.LeaderID, Kind: "snap-resp",
			SnapResp: &InstallSnapshotResp{Term: n.term, From: n.cfg.ID}})
		return
	}
	n.becomeFollower(req.Term, req.LeaderID)
	if req.LastIncludedIndex <= n.snapIndex {
		// 已覆盖，无需重复安装。
		n.replySnapshot(req.LeaderID)
		return
	}

	// 若本地日志在快照点之后仍有一致后缀，保留；否则全部丢弃。
	var remaining []LogEntry
	if n.lastIndex() > req.LastIncludedIndex && n.termAt(req.LastIncludedIndex) == req.LastIncludedTerm {
		remaining = n.sliceFrom(req.LastIncludedIndex + 1)
	}
	meta := &SnapshotMeta{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
		ClientSeq:         req.ClientSeq,
	}
	if err := n.st.saveSnapshot(meta, remaining); err != nil {
		n.logf("快照落盘失败: %v", err)
		return
	}
	n.lastSnap = meta
	n.snapIndex = req.LastIncludedIndex
	n.snapTerm = req.LastIncludedTerm
	n.entries = remaining

	// 状态机整体替换为快照内容。
	n.kv = make(map[string]string, len(req.Data))
	for k, v := range req.Data {
		n.kv[k] = v
	}
	n.clientSeq = make(map[string]uint64, len(req.ClientSeq))
	for k, v := range req.ClientSeq {
		n.clientSeq[k] = v
	}
	n.commitIndex = n.snapIndex
	n.lastApplied = n.snapIndex
	n.logf("安装快照完成: snapIndex=%d snapTerm=%d 状态机键数=%d 保留后缀=%d条 commit=%d",
		n.snapIndex, n.snapTerm, len(n.kv), len(n.entries), n.commitIndex)
	n.replySnapshot(req.LeaderID)
}

func (n *Node) replySnapshot(to string) {
	n.tr.Send(Message{From: n.cfg.ID, To: to, Kind: "snap-resp",
		SnapResp: &InstallSnapshotResp{Term: n.term, From: n.cfg.ID}})
}

// onInstallSnapshotResp 领导收到快照响应：把对端推进到快照点，继续增量复制。
func (n *Node) onInstallSnapshotResp(resp InstallSnapshotResp) {
	if resp.Term > n.term {
		n.becomeFollower(resp.Term, "")
		return
	}
	if n.role != Leader || n.lastSnap == nil {
		return
	}
	if n.matchIndex[resp.From] < n.lastSnap.LastIncludedIndex {
		n.matchIndex[resp.From] = n.lastSnap.LastIncludedIndex
		n.nextIndex[resp.From] = n.lastSnap.LastIncludedIndex + 1
	}
	n.sendAppendTo(resp.From)
}
