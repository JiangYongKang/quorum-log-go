package raft

// 选举与角色转换。核心不变式：
//   - 同一任期内一个节点最多投出一票（votedFor 先落盘再回复）；
//   - 获得多数票才成为领导，因此同一任期至多一个领导；
//   - 任期更大的消息会让任何节点退位为跟随者。

func (n *Node) resetElectionTimeout() {
	n.electionElapsed = 0
	span := n.cfg.ElectionTickMax - n.cfg.ElectionTickMin
	if span <= 0 {
		span = 1
	}
	n.electionTimeout = n.cfg.ElectionTickMin + n.rng.Intn(span)
}

func (n *Node) onTick() {
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTick {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.startElection()
	}
}

// becomeFollower 切换到跟随者；任期前进时先持久化硬状态。
func (n *Node) becomeFollower(term uint64, leaderID string) {
	oldRole := n.role
	n.role = Follower
	n.leaderID = leaderID
	if term > n.term {
		n.term = term
		n.votedFor = ""
		if err := n.st.saveHardState(HardState{CurrentTerm: n.term, VotedFor: n.votedFor}); err != nil {
			n.logf("落盘硬状态失败: %v", err)
		}
	}
	if oldRole != Follower {
		// 退位时未提交的客户端请求全部失败，客户端可重试。
		n.failAllPending(ErrNotLeader)
		n.logf("退位为跟随者: term=%d leader=%q commit=%d", n.term, leaderID, n.commitIndex)
	}
	n.resetElectionTimeout()
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.cfg.ID
	n.heartbeatElapsed = 0
	last := n.lastIndex()
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.cfg.ID] = last
	n.logf("当选领导: term=%d lastIndex=%d commit=%d", n.term, last, n.commitIndex)
	// 追加一条 no-op，使前任期的条目能按"只提交当前任期条目"规则尽快提交。
	n.appendLocal(Command{Op: "noop"})
	n.broadcastAppend()
}

func (n *Node) startElection() {
	n.role = Candidate
	n.term++
	n.votedFor = n.cfg.ID
	n.leaderID = ""
	if err := n.st.saveHardState(HardState{CurrentTerm: n.term, VotedFor: n.votedFor}); err != nil {
		n.logf("落盘硬状态失败: %v", err)
	}
	n.votes = map[string]bool{n.cfg.ID: true}
	n.resetElectionTimeout()
	n.logf("发起选举: term=%d lastIndex=%d lastTerm=%d", n.term, n.lastIndex(), n.lastTerm())
	req := &RequestVoteReq{
		Term:         n.term,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.lastTerm(),
	}
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		n.tr.Send(Message{From: n.cfg.ID, To: p, Kind: "rv-req", RVReq: req})
	}
	// 单节点集群立即当选。
	n.maybeWinElection()
}

func (n *Node) maybeWinElection() {
	if n.role != Candidate {
		return
	}
	granted := 0
	for range n.votes {
		granted++
	}
	if granted > len(n.cfg.Peers)/2 {
		n.becomeLeader()
	}
}

func (n *Node) onRequestVoteReq(req RequestVoteReq) {
	if req.Term < n.term {
		n.logf("拒绝投票给 %s: 对方任期 %d < 本地 %d", req.CandidateID, req.Term, n.term)
		n.sendVoteResp(req.CandidateID, false)
		return
	}
	if req.Term > n.term {
		n.becomeFollower(req.Term, "")
	}
	// 日志新旧比较：先比最后条目任期，再比长度。
	upToDate := req.LastLogTerm > n.lastTerm() ||
		(req.LastLogTerm == n.lastTerm() && req.LastLogIndex >= n.lastIndex())
	grant := (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate
	if grant {
		n.votedFor = req.CandidateID
		// 投票先落盘再回复，宕机重启后不会在同任期重复投票。
		if err := n.st.saveHardState(HardState{CurrentTerm: n.term, VotedFor: n.votedFor}); err != nil {
			n.logf("落盘硬状态失败: %v", err)
		}
		n.resetElectionTimeout()
	}
	n.logf("投票请求 from=%s term=%d 授予=%v (本地votedFor=%q upToDate=%v)",
		req.CandidateID, req.Term, grant, n.votedFor, upToDate)
	n.sendVoteResp(req.CandidateID, grant)
}

func (n *Node) sendVoteResp(to string, granted bool) {
	n.tr.Send(Message{From: n.cfg.ID, To: to, Kind: "rv-resp",
		RVResp: &RequestVoteResp{Term: n.term, Granted: granted, From: n.cfg.ID}})
}

func (n *Node) onRequestVoteResp(resp RequestVoteResp) {
	if resp.Term > n.term {
		n.becomeFollower(resp.Term, "")
		return
	}
	if n.role != Candidate || resp.Term != n.term || !resp.Granted {
		return
	}
	n.votes[resp.From] = true
	n.maybeWinElection()
}
