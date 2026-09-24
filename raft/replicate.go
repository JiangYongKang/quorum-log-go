package raft

// 日志复制与提交。核心不变式：
//   - 只有被多数节点复制的、且属于当前领导任期的条目才推进提交位点；
//   - 客户端写入带幂等标识：已应用或在途的重复请求不会追加新条目；
//   - 冲突条目先截断再追加，被覆盖的未提交条目永远不会被应用。

// onPropose 处理客户端写入（仅在事件循环内调用）。
func (n *Node) onPropose(req proposeReq) {
	cmd := req.cmd
	if n.role != Leader {
		req.reply <- proposeRes{err: ErrNotLeader}
		return
	}
	key := clientKey(cmd)
	// 幂等：已应用过的请求直接返回成功，不追加新条目。
	if cmd.ClientID != "" && cmd.Seq <= n.clientSeq[cmd.ClientID] {
		n.logf("幂等去重: client=%s seq=%d 已应用，跳过追加", cmd.ClientID, cmd.Seq)
		req.reply <- proposeRes{index: n.clientIndex[key]}
		return
	}
	// 幂等：日志中已存在该请求的条目（在途或重启恢复），挂到同一条目上等待。
	if idx, ok := n.clientIndex[key]; ok && cmd.ClientID != "" {
		n.logf("幂等去重: client=%s seq=%d 已在日志 index=%d，复用等待", cmd.ClientID, cmd.Seq, idx)
		n.pending[idx] = append(n.pending[idx], req.reply)
		return
	}
	idx := n.appendLocal(cmd)
	if cmd.ClientID != "" {
		n.clientIndex[key] = idx
	}
	n.pending[idx] = append(n.pending[idx], req.reply)
	n.broadcastAppend()
	// 单节点集群立即提交。
	n.advanceCommit()
}

// appendLocal 向本地日志追加条目并落盘，返回 index。
func (n *Node) appendLocal(cmd Command) uint64 {
	idx := n.lastIndex() + 1
	e := LogEntry{Term: n.term, Index: idx, Command: cmd}
	if err := n.st.appendEntries([]LogEntry{e}); err != nil {
		n.logf("WAL 追加失败: %v", err)
	}
	n.entries = append(n.entries, e)
	n.matchIndex[n.cfg.ID] = idx
	return idx
}

// broadcastAppend 向所有对等节点发送 AppendEntries（或快照）。
func (n *Node) broadcastAppend() {
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		n.sendAppendTo(p)
	}
}

// sendAppendTo 根据 nextIndex 决定发送增量日志还是快照。
func (n *Node) sendAppendTo(peer string) {
	next := n.nextIndex[peer]
	if next <= n.snapIndex {
		// 对端落后太多，日志已被压缩，改发快照。
		n.sendSnapshotTo(peer)
		return
	}
	req := &AppendEntriesReq{
		Term:         n.term,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: next - 1,
		PrevLogTerm:  n.termAt(next - 1),
		Entries:      n.sliceFrom(next),
		LeaderCommit: n.commitIndex,
	}
	n.tr.Send(Message{From: n.cfg.ID, To: peer, Kind: "ae-req", AEReq: req})
}

// onAppendEntriesReq 跟随者处理追加请求。
func (n *Node) onAppendEntriesReq(req AppendEntriesReq) {
	if req.Term < n.term {
		n.replyAppend(req.LeaderID, false, 0, 0)
		return
	}
	n.becomeFollower(req.Term, req.LeaderID)

	// 前序点落在快照里：裁剪掉已被快照覆盖的部分。
	entries := req.Entries
	prevIndex, prevTerm := req.PrevLogIndex, req.PrevLogTerm
	if prevIndex < n.snapIndex {
		trim := n.snapIndex - prevIndex
		if uint64(len(entries)) <= trim {
			// 整条请求都在快照范围内，只需推进提交位点。
			n.commitTo(min(req.LeaderCommit, n.snapIndex))
			n.replyAppend(req.LeaderID, true, 0, n.snapIndex)
			return
		}
		entries = entries[trim:]
		prevIndex, prevTerm = n.snapIndex, n.snapTerm
	}

	// 一致性检查。
	if prevIndex > n.lastIndex() {
		n.replyAppend(req.LeaderID, false, 0, n.lastIndex()+1)
		return
	}
	if t := n.termAt(prevIndex); t != prevTerm {
		// 快速回退：返回冲突任期的第一个 index。
		conflictTerm := t
		first := prevIndex
		for first > n.snapIndex+1 && n.termAt(first-1) == conflictTerm {
			first--
		}
		n.logf("日志冲突: prevIndex=%d 本地term=%d 领导term=%d 回退到 %d",
			prevIndex, t, prevTerm, first)
		n.replyAppendWithTerm(req.LeaderID, conflictTerm, first)
		return
	}

	// 逐条比对：发现冲突即截断，其后全部替换为领导日志。
	truncated := false
	for i, e := range entries {
		idx := prevIndex + 1 + uint64(i)
		if idx <= n.lastIndex() {
			if n.termAt(idx) != e.Term {
				keep := n.sliceFrom(n.snapIndex + 1)
				keep = keep[:idx-n.snapIndex-1]
				rest := entries[i:]
				if err := n.st.truncateAndAppend(keep, rest); err != nil {
					n.logf("WAL 截断重写失败: %v", err)
				}
				n.entries = append(keep, rest...)
				n.logf("覆盖冲突条目: 从 index=%d 起截断并追加 %d 条 (被覆盖的未提交条目不会对外呈现)",
					idx, len(rest))
				truncated = true
				break
			}
			// 已有相同条目，跳过。
		} else {
			rest := entries[i:]
			if err := n.st.appendEntries(rest); err != nil {
				n.logf("WAL 追加失败: %v", err)
			}
			n.entries = append(n.entries, rest...)
			break
		}
	}
	_ = truncated

	n.commitTo(min(req.LeaderCommit, n.lastIndex()))
	n.replyAppend(req.LeaderID, true, 0, n.lastIndex())
}

// commitTo 推进提交位点并应用。
func (n *Node) commitTo(to uint64) {
	if to > n.commitIndex {
		old := n.commitIndex
		n.commitIndex = min(to, n.lastIndex())
		n.logf("提交位点推进: %d -> %d (term=%d)", old, n.commitIndex, n.term)
	}
	n.applyTo(n.commitIndex)
	n.maybeSnapshot()
}

func (n *Node) replyAppend(to string, success bool, conflictTerm, conflictIndex uint64) {
	n.replyAppendFull(to, success, conflictTerm, conflictIndex, n.lastIndex())
}

func (n *Node) replyAppendWithTerm(to string, conflictTerm, conflictIndex uint64) {
	n.replyAppendFull(to, false, conflictTerm, conflictIndex, n.lastIndex())
}

func (n *Node) replyAppendFull(to string, success bool, conflictTerm, conflictIndex, match uint64) {
	n.tr.Send(Message{From: n.cfg.ID, To: to, Kind: "ae-resp",
		AEResp: &AppendEntriesResp{
			Term: n.term, From: n.cfg.ID, Success: success,
			ConflictTerm: conflictTerm, ConflictIndex: conflictIndex,
			MatchIndex: match,
		}})
}

// onAppendEntriesResp 领导处理追加响应。
func (n *Node) onAppendEntriesResp(resp AppendEntriesResp) {
	if resp.Term > n.term {
		n.becomeFollower(resp.Term, "")
		return
	}
	if n.role != Leader || resp.Term != n.term {
		return
	}
	peer := resp.From
	if resp.Success {
		if resp.MatchIndex > n.matchIndex[peer] {
			n.matchIndex[peer] = resp.MatchIndex
			n.nextIndex[peer] = resp.MatchIndex + 1
		}
		n.advanceCommit()
		return
	}
	// 失败：利用冲突信息快速回退 nextIndex。
	next := resp.ConflictIndex
	if resp.ConflictTerm != 0 {
		// 若本地有该任期的条目，跳到该任期最后一条之后。
		found := false
		for i := n.lastIndex(); i > n.snapIndex && i > 0; i-- {
			t := n.termAt(i)
			if t == resp.ConflictTerm {
				next = i + 1
				found = true
				break
			}
			if t < resp.ConflictTerm {
				break
			}
		}
		if !found {
			next = resp.ConflictIndex
		}
	}
	if next < 1 {
		next = 1
	}
	if next < n.nextIndex[peer] {
		n.nextIndex[peer] = next
	}
	n.sendAppendTo(peer)
}

// advanceCommit 领导推进提交位点：多数复制且属于当前任期。
func (n *Node) advanceCommit() {
	if n.role != Leader {
		return
	}
	for idx := n.lastIndex(); idx > n.commitIndex; idx-- {
		if n.termAt(idx) != n.term {
			continue // 只提交当前任期的条目，避免旧任期条目被错误提交
		}
		count := 0
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count > len(n.cfg.Peers)/2 {
			old := n.commitIndex
			n.commitIndex = idx
			n.logf("提交位点推进: %d -> %d (term=%d 多数确认)", old, idx, n.term)
			n.applyTo(n.commitIndex)
			n.maybeSnapshot()
		}
		break
	}
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
