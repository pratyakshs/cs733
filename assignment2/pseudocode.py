State : Follower

on Append(data):
	Send(sm.leaderId, Append(data))


on Timeout:
    sm.State = Candidate
    Alarm(time.now() + rand(1.0, 2.0) * ELECTION_TIMEOUT)

    sm.term += 1
    sm.votedFor = sm.Id
    sm.voteGranted = {}
    sm.voteGranted[sm.Id] = True	# all other entries false

    for each peer:
   		action = Send(peer, VoteReq(sm.Id, sm.term, sm.Id, sm.lastLogIndex, 
   					  sm.lastLogTerm))


on AppendEntriesReq(term, leaderId, prevLogIndex, prevLogTerm, entries[], 
					leaderCommit):
	if sm.term < msg.term:
		sm.term = msg.term
		sm.votedFor = nil
		sm.leaderId = msg.leaderId

	if sm.currentTerm > msg.term
		action = Send(msg.leaderId, AppendEntriesResp(sm.currentTerm, 
					  success=no, nil))

	else:
		Alarm(time.now() + rand(1.0, 2.0) * ELECTION_TIMEOUT)

		check = (msg.prevLogIndex == 0 or 
				 (msg.prevIndex <= len(sm.log) and 
				  sm.log[m.prevIndex].term == msg.prevLogTerm))

		if check:
	   		sm.lastLogIndex = msg.prevLogIndex + len(entries)
			i = msg.prevLogIndex
			for j = 1..len(msg.entries):
				i += 1
				LogStore(i, msg.entries[j])
	   		sm.lastLogTerm = sm.CurrentTerm
			action = Send(msg.leaderID, AppendEntriesResp(sm.currentTerm, 
						  sm.lastLogIndex, success =yes))
			commitIndex = min(msg.leaderCommit, sm.lastLogIndex)
			matchIndex = sm.lastLogIndex
		else:
			matchIndex = 0
		action = Send(msg.leaderId, AppendEntriesResp(sm.currentTerm, 
					 success=yes, matchIndex))


on AppendEntriesResp(from, term, index, success):
	if sm.term < msg.term:
		sm.term = msg.term
		sm.votedFor = nil


on VoteReq(from, term, candidateId, lastLogIndex, lastLogTerm):
	if sm.term < msg.term:
		sm.term = msg.term
		sm.votedFor = nil
		# increment election alarm?
	if sm.term == msg.term && 
		(sm.votedFor == nil or sm.votedFor == msg.candidateId):
		if (msg.lastLogTerm > sm.logTerm[-1] or 
			(msg.lastLogTerm == sm.logTerm[-1] and 
				msg.lastLogIndex >= len(sm.log))):
			sm.term = msg.term
			sm.votedFor = msg.candidateId
			action = Send(msg.from, VoteResp(sm.term, voteGranted=yes))
	else: #reject vote:
		action = Send(msg.from, VoteResp(sm.term, voteGranted=no))

	Alarm(time.now() + rand(1.0, 2.0) * ELECTION_TIMEOUT)


on VoteResp(from, term, voteGranted):
	if sm.term < msg.term:
		sm.term = msg.term
		sm.votedFor = nil

###############################################################################

State: Candidate


on Append(data):
	Send(sm.leaderId, Append(data))


on Timeout:
    Alarm(time.now() + rand(1.0, 2.0) * ELECTION_TIMEOUT)

    sm.term += 1
    sm.votedFor = sm.Id
    sm.voteGranted = {}
    sm.voteGranted[sm.Id] = True	# all other entries false

    for each peer:
   		action = Send(peer, VoteReq(sm.Id, sm.term, sm.Id, sm.lastLogIndex, 
   					  sm.lastLogTerm))


on AppendEntriesReq(term, leaderId, prevLogIndex, prevLogTerm, entries[], 
					leaderCommit):
	if sm.term < msg.term:
		sm.term = msg.term
		sm.votedFor = nil
		sm.leaderId = msg.leaderId
		sm.State = Follower

	if sm.currentTerm > msg.term
		action = Send(msg.leaderId, AppendEntriesResp(sm.currentTerm, 
					  success=no, nil))

	else:
		Alarm(time.now() + rand(1.0, 2.0) * ELECTION_TIMEOUT)

		check = (msg.prevLogIndex == 0 or (msg.prevIndex <= len(sm.log) and 
			     sm.log[m.prevIndex].term == msg.prevLogTerm))

		if check:
	   		sm.lastLogIndex = msg.prevLogIndex + len(entries)
			i = msg.prevLogIndex
			for j = 1..len(msg.entries):
				i += 1
				LogStore(i, msg.entries[j])
	   		sm.lastLogTerm = sm.CurrentTerm
			action = Send(msg.leaderID, AppendEntriesResp(sm.currentTerm, 
						  sm.lastLogIndex, success =yes))
			commitIndex = min(msg.leaderCommit, sm.lastLogIndex)
			matchIndex = sm.lastLogIndex
		else:
			matchIndex = 0
		action = Send(msg.leaderId, AppendEntriesResp(sm.currentTerm, 
					  success=yes, matchIndex))


on AppendEntriesResp(from, term, index, success):
	if sm.term < msg.term:
		sm.term = msg.term
		sm.votedFor = nil


on VoteReq(from, term, candidateId, lastLogIndex, lastLogTerm):
	if sm.term < msg.term:
		sm.State = Follower
		sm.term = msg.term
		sm.votedFor = nil
		# increment election alarm?
	if sm.term == msg.term && 
		(sm.votedFor == nil or sm.votedFor == msg.candidateId):
		if (msg.lastLogTerm > sm.logTerm[-1] or 
			(msg.lastLogTerm == sm.logTerm[-1] and 
				msg.lastLogIndex >= len(sm.log))):
			sm.term = msg.term
			sm.votedFor = msg.candidateId
			action = Send(msg.from, VoteResp(sm.term, voteGranted=yes))
	else: #reject vote:
		action = Send(msg.from, VoteResp(sm.term, voteGranted=no))

	Alarm(time.now() + rand(1.0, 2.0) * ELECTION_TIMEOUT)


on VoteResp(from, term, voteGranted):
	if sm.currentTerm < msg.term:
		sm.term = msg.term
		sm.votedFor = nil
		sm.State = Follower

    if sm.term == msg.term:
    	sm.voteGranted[sm.from] = msg.voteGranted

    if sum(voteGranted) > NUM_SERVERS / 2:
    	sm.State = Leader
    	sm.leaderId = sm.Id
    	for each peer:
    		sm.nextIndex[peer] = len(sm.log) + 1
    		sm.matchIndex[peer] = 0
			send(peer, AppendEntriesReq(sm.Id, sm.term, sm.lastLogIndex, 
				 sm.lastLogTerm, [], sm.commitIndex))
		Alarm(time.now() + rand(1.0, 2.0) * ELECTION_TIMEOUT)



###############################################################################

State : Leader


on Append(data)
	LogStore(sm.lastLogIndex + 1, data)
	for each peer:
   		action = Send(peer, AppendEntriesReq(sm.Id, sm.term, sm.lastLogIndex, 
   					  sm.lastLogTerm, data, sm.commitIndex))
    	sm.lastLogIndex += 1
    	sm.lastLogTerm = sm.term


on Timeout
	for each peer:
       send(peer, AppendEntriesReq(sm.Id, sm.term, sm.lastLogIndex, sm.lastLogTerm,
       	    [], sm.commitIndex))
	Alarm(time.now() + rand(1.0, 2.0) * ELECTION_TIMEOUT)


on AppendEntriesReq(term, leaderId, prevLogIndex, prevLogTerm, entries[], 
					leaderCommit):
	if sm.term < msg.term:
		sm.term = msg.term
		sm.votedFor = nil
		sm.leaderId = msg.leaderId
		sm.State = Follower

	if sm.currentTerm > msg.term
		action = Send(msg.leaderId, AppendEntriesResp(sm.currentTerm, 
					  success=no, nil))

	else:
		Alarm(time.now() + rand(1.0, 2.0) * ELECTION_TIMEOUT)

		check = (msg.prevLogIndex == 0 or (msg.prevIndex <= len(sm.log) and 
			     sm.log[m.prevIndex].term == msg.prevLogTerm))

		if check:
	   		sm.lastLogIndex = msg.prevLogIndex + len(entries)
			i = msg.prevLogIndex
			for j = 1..len(msg.entries):
				i += 1
				LogStore(i, msg.entries[j])
	   		sm.lastLogTerm = sm.CurrentTerm
			action = Send(msg.leaderID, AppendEntriesResp(sm.currentTerm, 
						  sm.lastLogIndex, success =yes))
			commitIndex = min(msg.leaderCommit, sm.lastLogIndex)
			matchIndex = sm.lastLogIndex
		else:
			matchIndex = 0
		action = Send(msg.leaderId, AppendEntriesResp(sm.currentTerm, 
					  success=yes, matchIndex))


on AppendEntriesResp(from, term, index, success):
	if sm.term < msg.term:
		sm.term = msg.term
		sm.votedFor = nil
		sm.leaderId = msg.leaderId
		sm.State = Follower
	
	if sm.term == msg.term:
		if msg.success:
			sm.matchIndex[msg.from] = msg.index
			sm.nextIndex[msg.from] = msg.index + 1

			if sm.matchIndex[msg.from] < len(sm.log):
				sm.nextIndex[msg.from] = len(log)
				prevLogIndex = sm.nextIndex[msg.from] - 1
				prevLogTerm = sm.log[prevLogIndex].term
				funcall = AppendEntriesReq(sm.term, sm.Id, prevLogIndex, prevLogTerm,
										   entries[sm.nextIndex[msg.from]:len(log)],
										   leaderCommit)
				action = Send(msg.from, funcall)

			cnt =0
			for each peer:
				if peer.Id != sm.Id && sm.matchIndex[peer] > sm.commitIndex:
					cnt += 1
			if cnt > sm.numPeers / 2 
			sm.commitIndex++
			Commit(index, data, err)

		else:
			sm.nextIndex[msg.from] = max(1, sm.nextIndex[msg.from] - 1)
			action = Send(msg.from, 
						  AppendEntriesReq(sm.Id, sm.term, 
						  sm.nextIndex[msg.from], sm.log[sm.nextIndex[msg.from]].term,
						  sm.log[nextIndex[msg.from]:len(sm.log)], 
						  sm.commitIndex))


on VoteResp(from, term, voteGranted):
	if sm.currentTerm < msg.term:
		sm.term = msg.term
		sm.votedFor = nil
		sm.State = Follower


on VoteReq(from, term, candidateId, lastLogIndex, lastLogTerm):
	if sm.term < msg.term:
		sm.State = Follower
		sm.term = msg.term
		sm.votedFor = nil
		Alarm(time.now() + rand(1.0, 2.0) * ELECTION_TIMEOUT)

	if sm.term == msg.term && 
		(sm.votedFor == nil or sm.votedFor == msg.candidateId):
		if (msg.lastLogTerm > sm.logTerm[-1] or 
			(msg.lastLogTerm == sm.logTerm[-1] and 
				msg.lastLogIndex >= len(sm.log))):
			sm.term = msg.term
			sm.votedFor = msg.candidateId
			action = Send(msg.from, VoteResp(sm.term, voteGranted=yes))
	else: #reject vote:
		action = Send(msg.from, VoteResp(sm.term, voteGranted=no))

##############################################################################
