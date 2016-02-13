package main

// package raft

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

const (
	ELECTION_TIMEOUT  = 150 // milliseconds
	HEARTBEAT_TIMEOUT = 50  // milliseconds
	NUM_SERVERS       = 5
)

type LogEntry struct {
	Data      []byte
	Committed bool
	Term      int
	LogIndex  int
}

type AppendMsg struct {
	Data []byte
}

type AppendEntriesReq struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesResp struct {
	From       int
	Term       int
	MatchIndex int
	Success    bool
}

type VoteReq struct {
	From         int
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type VoteResp struct {
	From        int
	Term        int
	VoteGranted bool
}

type StateMachine struct {
	CurrentTerm  int
	LeaderID     int
	ServerID     int
	State        string
	VoteGranted  map[int]bool
	VotedFor     int
	NextIndex    map[int]int
	MatchIndex   map[int]int
	CommitIndex  int
	Log          []LogEntry
	clientCh     chan interface{}
	netCh        chan interface{}
	actionCh     chan interface{}
	Timer        *time.Timer
	Mutex        sync.RWMutex
	PeerChan     map[int](chan interface{})
	LastLogIndex int //??
	LastLogTerm  int //??
}

func (sm *StateMachine) getLogTerm(i int) int {
	if i >= 0 {
		return sm.Log[i].Term
	} else {
		return sm.Log[len(sm.Log)+i].Term
	}
}

func (sm *StateMachine) doAppendEntriesReq(msg AppendEntriesReq) {

	if sm.CurrentTerm < msg.Term {
		sm.CurrentTerm = msg.Term
		sm.VotedFor = -1
		sm.LeaderID = msg.LeaderID
		sm.State = "Follower"
	}

	if (sm.CurrentTerm) > msg.Term {
		// action = Send(msg.leaderId, AppendEntriesResp(sm.currentTerm, success=no, nil))
	} else {
		// Alarm(time.now() + rand(1.0, 2.0) * ELECTION_TIMEOUT)
		check := msg.PrevLogIndex == -1 || (msg.PrevLogIndex <= len(sm.Log)-1 && sm.getLogTerm(msg.PrevLogIndex) == msg.PrevLogTerm)
		var matchIndex int
		if check {
			sm.LastLogIndex = msg.PrevLogIndex + len(msg.Entries)
			i := msg.PrevLogIndex
			for j := 0; j < len(msg.Entries); j += 1 {
				i += 1
				// LogStore(i, msg.entries[j])
			}
			sm.LastLogTerm = sm.CurrentTerm
			// action = Send(msg.leaderID, AppendEntriesResp(sm.currentTerm,
			// 			  sm.lastLogIndex, success =yes))
			if msg.LeaderCommit < sm.LastLogIndex {
				sm.CommitIndex = msg.LeaderCommit
			} else {
				sm.CommitIndex = sm.LastLogIndex
			}
			matchIndex = sm.LastLogIndex
		} else {
			matchIndex = 0
		}
		fmt.Println(matchIndex) //??
		// action = Send(msg.leaderId, AppendEntriesResp(sm.currentTerm,
		// 			 success=yes, matchIndex))
	}
}

func (sm *StateMachine) doAppendEntriesResp(msg AppendEntriesResp) {
	if sm.CurrentTerm < msg.Term {
		sm.CurrentTerm = msg.Term
		sm.VotedFor = -1
		sm.State = "Follower"
	}

	if sm.State == "Leader" {

		// if sm.CurrentTerm < msg.Term:
		// sm.LeaderID = msg.LeaderID

		if sm.CurrentTerm == msg.Term {
			if msg.Success {
				sm.MatchIndex[msg.From] = msg.MatchIndex
				sm.NextIndex[msg.From] = msg.MatchIndex + 1

				if sm.MatchIndex[msg.From] < len(sm.Log)-1 { //??
					sm.NextIndex[msg.From] = len(sm.Log) - 1
					prevLogIndex := sm.NextIndex[msg.From] - 1
					prevLogTerm := sm.getLogTerm(prevLogIndex)
					fmt.Println(prevLogTerm)
					// funcall = AppendEntriesReq(sm.term, sm.Id, prevLogIndex, prevLogTerm,
					// entries[sm.nextIndex[msg.from]:len(log)],
					// leaderCommit)
					// action = Send(msg.from, funcall)
				}

				cnt := 0
				for peer, _ := range sm.PeerChan {
					if peer != sm.ServerID && sm.MatchIndex[peer] > sm.CommitIndex {
						cnt += 1
					}
				}
				if cnt > NUM_SERVERS/2 {
					sm.CommitIndex += 1
					// Commit(index, data, err)
				}
			} else {
				sm.NextIndex[msg.From] -= 1
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

func (sm *StateMachine) doVoteReq(msg VoteReq) {
	if sm.CurrentTerm < msg.Term {
		sm.State = "Follower"
		sm.CurrentTerm = msg.Term
		sm.VotedFor = -1
		// Alarm(time.now() + rand(1.0, 2.0) * ELECTION_TIMEOUT)
	}

	if (sm.CurrentTerm == msg.Term) && (sm.VotedFor == -1 || sm.VotedFor == msg.CandidateId) {
		if msg.LastLogTerm > sm.getLogTerm(-1) || (msg.LastLogTerm == sm.getLogTerm(-1) && msg.LastLogIndex >= len(sm.Log)-1) { // ??
			sm.CurrentTerm = msg.Term
			sm.VotedFor = msg.CandidateId
			// action = Send(msg.From, VoteResp(sm.CurrentTerm, voteGranted=yes))
		}
	} else { // reject vote:
		// action = Send(msg.from, VoteResp(sm.term, voteGranted=no))
	}
}

func (sm *StateMachine) doVoteResp(msg VoteResp) {
	if sm.CurrentTerm < msg.Term {
		sm.CurrentTerm = msg.Term
		sm.VotedFor = -1
		sm.State = "Follower"
	}

	if sm.State == "Candidate" {

		if sm.CurrentTerm == msg.Term {
			sm.VoteGranted[msg.From] = msg.VoteGranted
		}

		if countVotes(sm.VoteGranted) > NUM_SERVERS/2 {
			sm.State = "Leader"
			sm.LeaderID = sm.ServerID
			for peer := range sm.PeerChan {
				sm.NextIndex[peer] = len(sm.Log) // ??
				sm.MatchIndex[peer] = -1
				// send(peer, AppendEntriesReq(sm.Id, sm.term, sm.lastLogIndex, sm.lastLogTerm, [], sm.commitIndex))
			}
			// Alarm(time.now() + rand(1.0, 2.0) * ELECTION_TIMEOUT)
		}
	}
}

func countVotes(VoteGranted map[int]bool) (count int) {
	count = 0
	for _, vote := range VoteGranted {
		if vote {
			count += 1
		}
	}
	return
}

func (sm *StateMachine) eventLoop() {
	for {
		select {
		case appendMsg := <-sm.clientCh:
			t := reflect.TypeOf(appendMsg)
			fmt.Println(t)

		case peerMsg := <-sm.netCh:
			t := reflect.TypeOf(peerMsg)
			switch t.Name() {
			case "AppendEntriesReq":
				go sm.doAppendEntriesReq(peerMsg.(AppendEntriesReq))
			case "AppendEntriesResp":
				go sm.doAppendEntriesResp(peerMsg.(AppendEntriesResp))
			case "VoteResp":
				go sm.doVoteResp(peerMsg.(VoteResp))
			case "VoteReq":
				go sm.doVoteReq(peerMsg.(VoteReq))
			}
		}
	}
}

func NewStateMachine() *StateMachine {
	sm := StateMachine{
		VoteGranted: make(map[int]bool),
		NextIndex:   make(map[int]int),
		MatchIndex:  make(map[int]int),
		clientCh:    make(chan interface{}),
		netCh:       make(chan interface{}),
		actionCh:    make(chan interface{}),
		VotedFor:    -1,
	}
	return &sm
}

func main() {
	sm1 := NewStateMachine()
	go sm1.eventLoop()

	sm2 := NewStateMachine()
	go sm2.eventLoop()

	// sm1.PeerChan = sm2.netCh
	// sm2.PeerChan = sm1.netCh

	// vr := VoteResp{From: 1, Term: 2, VoteGranted: true}

	// sm2.netCh <- vr
	// sm1.PeerChan <- vr
	// time.Sleep(10 * time.Second)

	// go func() {
	// 	vR := <-sm.netCh
	// 	fmt.Println(vR)
	// }()
	// sm.netCh <- vr
}
