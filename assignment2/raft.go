package raft

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

const (
	// ElectionTimeout constant in millisecond units
	ElectionTimeout = 150

	// HeartbeatTimeout constant in millisecond units
	HeartbeatTimeout = 50

	// NumServers is the hardcoded number of Raft servers
	NumServers = 5
)

// LogEntry is the type for a single entry in the log
type LogEntry struct {
	Data      []byte
	Committed bool
	Term      int
	LogIndex  int
}

// AppendMsg is the type for a log append request from a client
type AppendMsg struct {
	Data []byte
}

// AppendEntriesReq is the type for an append entries request from a leader
type AppendEntriesReq struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesResp is the type for the response of an append entries request sent back to the leader
type AppendEntriesResp struct {
	From       int
	Term       int
	MatchIndex int
	Success    bool
}

// VoteReq is the type for the vote request send by a candidate to its peers
type VoteReq struct {
	From        int
	Term        int
	CandidateID int
	LastIndex   int
	LastTerm    int
}

// VoteResp is the type for the response of a vote request sent back to the candidate
type VoteResp struct {
	From        int
	Term        int
	VoteGranted bool
}

// LogStore is the type representing a log store action
type LogStore struct {
	From  int
	Index int
	Data  []byte
}

// StateMachine represents a single raft node
type StateMachine struct {
	Term        int
	LeaderID    int
	ServerID    int
	State       string
	VoteGranted map[int]bool
	VotedFor    int
	NextIndex   map[int]int
	MatchIndex  map[int]int
	CommitIndex int
	Log         []LogEntry
	clientCh    chan interface{}
	netCh       chan interface{}
	actionCh    chan interface{}
	Timer       *time.Timer
	Mutex       sync.RWMutex
	PeerChan    map[int](chan interface{})
	LastIndex   int //??
	LastTerm    int //??
}

func (sm *StateMachine) getLogTerm(i int) int {
	if i >= 0 {
		return sm.Log[i].Term
	}
	return sm.Log[len(sm.Log)+i].Term	//FIXME: not required
}

func (sm *StateMachine) stepDown(newTerm int) {
	sm.Term = newTerm
	sm.VotedFor = -1
	sm.NextIndex = nil
	sm.MatchIndex = nil
	sm.State = "Follower"
	sm.VoteGranted = nil
}

func (sm *StateMachine) countVotes() int {
	count := 0
	for _, vote := range sm.VoteGranted {
		if vote {
			count++
		}
	}
	return count
}

func (sm *StateMachine) onAppendEntriesReq(msg AppendEntriesReq) {

	if sm.Term < msg.Term {
		sm.stepDown(msg.Term)
		sm.LeaderID = msg.LeaderID
	}

	if (sm.Term) > msg.Term {
		sm.actionCh <- AppendEntriesResp{From: sm.ServerID, Term: sm.Term, MatchIndex: -1, Success: false}
	} else {
		//TODO: send alarm message
		// Alarm(time.now() + rand(1.0, 2.0) * ElectionTimeout)
		check := msg.PrevLogIndex == -1
		if !check {
			check = (msg.PrevLogIndex < len(sm.Log) && sm.getLogTerm(msg.PrevLogIndex) == msg.PrevLogTerm)
		}

		var matchIndex int
		if check {
			sm.LastIndex = msg.PrevLogIndex + len(msg.Entries)
			i := msg.PrevLogIndex
			for _, entry := range msg.Entries {
				i++
				//TODO: check if term at index i is different from the new log entry's term
				sm.actionCh <- LogStore{From: sm.ServerID, Index: i, Data: entry.Data}
				//TODO: write code for actual log store
			}
			sm.LastTerm = sm.Term
			if msg.LeaderCommit < sm.LastIndex {
				sm.CommitIndex = msg.LeaderCommit
			} else {
				sm.CommitIndex = sm.LastIndex
			}
			matchIndex = sm.LastIndex
		} else {
			matchIndex = -1
		}
		sm.actionCh <- AppendEntriesResp{From: sm.ServerID, Term: sm.Term, MatchIndex: matchIndex, Success: true}
	}
}

func (sm *StateMachine) onAppendEntriesResp(msg AppendEntriesResp) {
	if sm.Term < msg.Term {
		sm.stepDown(msg.Term)
	}

	if sm.State == "Leader" {
		//FIXME: should update sm.LeaderID if response if from higher term
		// if sm.Term < msg.Term:
		// sm.LeaderID = msg.LeaderID

		if sm.Term == msg.Term {
			if msg.Success {
				sm.MatchIndex[msg.From] = msg.MatchIndex
				sm.NextIndex[msg.From] = msg.MatchIndex + 1

				if sm.MatchIndex[msg.From] < len(sm.Log)-1 { //??
					sm.NextIndex[msg.From] = len(sm.Log) - 1
					prevLogIndex := sm.NextIndex[msg.From] - 1
					prevLogTerm := sm.getLogTerm(prevLogIndex)
					sm.actionCh <- AppendEntriesReq{Term: sm.Term, LeaderID: sm.ServerID, PrevLogIndex: prevLogIndex,
						PrevLogTerm: prevLogTerm, Entries: sm.Log[sm.NextIndex[msg.From]:len(sm.Log)],
						LeaderCommit: sm.CommitIndex}
				}

				cnt := 0
				for peer := range sm.PeerChan {
					if peer != sm.ServerID && sm.MatchIndex[peer] > sm.CommitIndex {
						cnt++
					}
				}
				if cnt > NumServers/2 {
					sm.CommitIndex++
					// Commit(index, data, err)
				}
			} else {
				sm.NextIndex[msg.From]--
				if sm.NextIndex[msg.From] < 0 {
					sm.NextIndex[msg.From] = 0
				}
				// action = Send(msg.from,
				// AppendEntriesReq(sm.Id, sm.term,
				// sm.nextIndex[msg.from], sm.log[sm.nextIndex[msg.from]].term,
				// sm.log[nextIndex[msg.from]:len(sm.log)],
				// sm.commitIndex))
			}
		}
	}
}

func (sm *StateMachine) onVoteReq(msg VoteReq) {
	if sm.Term < msg.Term {
		sm.State = "Follower"
		sm.Term = msg.Term
		sm.VotedFor = -1
		// Alarm(time.now() + rand(1.0, 2.0) * ElectionTimeout)
	}

	if (sm.Term == msg.Term) && (sm.VotedFor == -1 || sm.VotedFor == msg.CandidateID) {
		if msg.LastTerm > sm.getLogTerm(-1) || (msg.LastTerm == sm.getLogTerm(-1) && msg.LastIndex >= len(sm.Log)-1) { // ??
			sm.Term = msg.Term
			sm.VotedFor = msg.CandidateID
			// action = Send(msg.From, VoteResp(sm.Term, voteGranted=yes))
		}
	} else { // reject vote:
		// action = Send(msg.from, VoteResp(sm.term, voteGranted=no))
	}
}

func (sm *StateMachine) onVoteResp(msg VoteResp) {
	if sm.Term < msg.Term {
		sm.Term = msg.Term
		sm.VotedFor = -1
		sm.State = "Follower"
	}

	if sm.State == "Candidate" {

		if sm.Term == msg.Term {
			sm.VoteGranted[msg.From] = msg.VoteGranted
		}

		if sm.countVotes() > NumServers/2 {
			sm.State = "Leader"
			sm.LeaderID = sm.ServerID
			for peer := range sm.PeerChan {
				sm.NextIndex[peer] = len(sm.Log) // ??
				sm.MatchIndex[peer] = -1
				// send(peer, AppendEntriesReq(sm.Id, sm.term, sm.LastIndex, sm.LastTerm, [], sm.commitIndex))
			}
			// Alarm(time.now() + rand(1.0, 2.0) * ElectionTimeout)
		}
	}
}

//func (sm *StateMachine) onTimeout() {
//
//	switch sm.State {
//	case "Leader":
//		for peer, _ := range sm.PeerChan {
//	       send(peer, AppendEntriesReq(sm.Id, sm.term, sm.LastIndex, sm.LastTerm,
//	       	    [], sm.commitIndex))
//	   }
//		// Alarm(time.now() + rand(1.0, 2.0) * ElectionTimeout)
//	case "Candidate", "Follower":
//		sm.State = "Candidate"
//    // Alarm(time.now() + rand(1.0, 2.0) * ElectionTimeout)
//
//	    sm.Term += 1
//	    sm.VotedFor = sm.ServerID
//			i := msg.PrevLogIndex
//			for j := 0; j < len(msg.Entries); j += 1 {
//				i += 1
//	    sm.VoteGranted = make(map[int]bool)
//	    sm.voteGranted[sm.ServerID] = true	// all other entries false
//
//	    for each peer:
//	   		action = Send(peer, VoteReq(sm.Id, sm.term, sm.Id, sm.LastIndex,
//	   					  sm.LastTerm))
//
//	}
//
//}

func (sm *StateMachine) eventLoop() {
	select {
	case appendMsg := <-sm.clientCh:
		t := reflect.TypeOf(appendMsg)
		fmt.Println(t)

	case peerMsg := <-sm.netCh:
		t := reflect.TypeOf(peerMsg)
		switch t.Name() {
		case "AppendEntriesReq":
			go sm.onAppendEntriesReq(peerMsg.(AppendEntriesReq))
		case "AppendEntriesResp":
			go sm.onAppendEntriesResp(peerMsg.(AppendEntriesResp))
		case "VoteResp":
			go sm.onVoteResp(peerMsg.(VoteResp))
		case "VoteReq":
			go sm.onVoteReq(peerMsg.(VoteReq))
		}
	}

}

// NewStateMachine creates a fresh Raft state machine with the given parameters
func NewStateMachine(term int, leaderID int, serverID int, state string) (*StateMachine, error) {
	switch state {
	case "Follower", "Candidate", "Leader":
		sm := StateMachine{
			Term:        term,
			LeaderID:    leaderID,
			ServerID:    serverID,
			State:       state,
			VoteGranted: nil,
			NextIndex:   nil,
			MatchIndex:  nil,
			clientCh:    make(chan interface{}),
			netCh:       make(chan interface{}),
			actionCh:    make(chan interface{}),
			VotedFor:    -1,
			CommitIndex: -1,
		}
		return &sm, nil
	}
	return &StateMachine{}, errors.New("Invalid state parameter")
}
