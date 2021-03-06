package main

import (
	"errors"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/cs733-iitb/log"
)

const (
	// ElectionTimeout constant in millisecond units
	ElectionTimeout = 2000

	// HeartbeatTimeout constant in millisecond units
	HeartbeatTimeout = 250

	// NumServers is the hardcoded number of Raft servers
	NumServers = 5
)

var err_redirect = errors.New("Commit error - server not a leader. Redirect request...")

// LogEntry is the type for a single entry in the log
type LogEntry struct {
	Data []byte
	Term int
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

// Timeout is the type for an election timeout or heartbeat timeout message to a server
type Timeout struct {
}

// LogStore is the type representing a log store action
type LogStore struct {
	Index int
	Entry LogEntry
}

// Alarm is the type representing a Alarm reset message(action)
type Alarm struct {
	AlarmTime time.Duration
}

// Send is the type representing a Send-message action
type Send struct {
	To      int
	Message interface{}
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
	PeerList    []int
	LastIndex   int //TODO: check that this is updated properly
	LastTerm    int //TODO: check that this is updated properly
}

func (sm *StateMachine) getLogTerm(i int) int {
	if i >= 0 {
		return sm.Log[i].Term
	}
	return -1
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

func (sm *StateMachine) LogIt(index int, entry LogEntry) {
	if index < len(sm.Log) {
		sm.Log[index] = entry
	} else {
		sm.Log = append(sm.Log, make([]LogEntry, index-len(sm.Log)+1)...)
		sm.Log[index] = entry
	}
	sm.actionCh <- LogStore{Index: index, Entry: sm.Log[index]}
}

func snoozeAlarmTime(n int32) time.Duration {
	return time.Duration(n+rand.Int31n(n)) * time.Millisecond
}

func (sm *StateMachine) onAppendEntriesReq(msg AppendEntriesReq) {
	//fmt.Println(sm.ServerID, "onAppendEntriesReq", msg)
	if sm.Term < msg.Term {
		sm.stepDown(msg.Term)
		sm.LeaderID = msg.LeaderID
	}

	if (sm.Term) > msg.Term {
		sm.actionCh <- Send{msg.LeaderID,
			AppendEntriesResp{From: sm.ServerID, Term: sm.Term, MatchIndex: -1, Success: false}}
	} else {
		sm.LeaderID = msg.LeaderID
		sm.actionCh <- Alarm{AlarmTime: snoozeAlarmTime(ElectionTimeout)}
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
				sm.LogIt(i, entry)
				//TODO: write code for actual log store
			}
			sm.LastTerm = sm.Term
			oldCommitIndex := sm.CommitIndex
			if msg.LeaderCommit < sm.LastIndex {
				sm.CommitIndex = msg.LeaderCommit
			} else {
				sm.CommitIndex = sm.LastIndex
			}
			matchIndex = sm.LastIndex
			//FIXME: for loop or single action?
			for i = oldCommitIndex + 1; i <= sm.CommitIndex; i++ {
				sm.actionCh <- CommitInfo{Data: sm.Log[i].Data, Index: int64(i), Err: nil}
			}
		} else {
			matchIndex = -1
		}
		sm.actionCh <- Send{msg.LeaderID,
			AppendEntriesResp{From: sm.ServerID, Term: sm.Term, MatchIndex: matchIndex, Success: check}}
	}
}

func (sm *StateMachine) onAppendEntriesResp(msg AppendEntriesResp) {
	//fmt.Println(sm.ServerID, "onAppendEntriesResp", msg)
	if sm.Term < msg.Term {
		sm.stepDown(msg.Term)
		sm.actionCh <- Alarm{AlarmTime: snoozeAlarmTime(ElectionTimeout)}
	}

	if sm.State == "Leader" {
		//FIXME: should update sm.LeaderID if response if from higher term
		// if sm.Term < msg.Term:
		// sm.LeaderID = msg.LeaderID

		if sm.Term == msg.Term {
			if msg.Success {
				sm.MatchIndex[msg.From] = msg.MatchIndex
				sm.NextIndex[msg.From] = msg.MatchIndex + 1

				var matchIndices []int
				for _, index := range sm.MatchIndex {
					matchIndices = append(matchIndices, index)
				}
				matchIndices = append(matchIndices, len(sm.Log)-1)
				sort.Ints(matchIndices)
				n := matchIndices[NumServers/2]
				if sm.getLogTerm(n) == sm.Term {
					oldCommitIndex := sm.CommitIndex
					sm.CommitIndex = n
					for i := oldCommitIndex + 1; i <= sm.CommitIndex; i++ {
						sm.actionCh <- CommitInfo{Data: sm.Log[i].Data, Index: int64(i), Err: nil}
					}
				}

				if sm.MatchIndex[msg.From] < len(sm.Log)-1 {
					prevLogIndex := sm.NextIndex[msg.From] - 1
					prevLogTerm := sm.getLogTerm(prevLogIndex)
					req := AppendEntriesReq{Term: sm.Term, LeaderID: sm.ServerID,
						PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
						Entries:      sm.Log[sm.NextIndex[msg.From]:len(sm.Log)],
						LeaderCommit: sm.CommitIndex}
					sm.actionCh <- Send{msg.From, req}
				}
			} else {
				sm.NextIndex[msg.From]--
				if sm.NextIndex[msg.From] < 0 {
					sm.NextIndex[msg.From] = 0
				}
				prevLogIndex := sm.NextIndex[msg.From] - 1
				prevLogTerm := sm.getLogTerm(prevLogIndex)
				msg := Send{msg.From, AppendEntriesReq{Term: sm.Term, LeaderID: sm.ServerID,
					PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
					Entries:      sm.Log[sm.NextIndex[msg.From]:len(sm.Log)],
					LeaderCommit: sm.CommitIndex}}
				sm.actionCh <- msg
			}
		}
	}
}

func (sm *StateMachine) onVoteReq(msg VoteReq) {
	//fmt.Println(sm.ServerID, "OnVoteReq", msg)
	if sm.Term < msg.Term {
		sm.stepDown(msg.Term)
	}

	var voteGranted bool
	canVote := (sm.Term == msg.Term) && (sm.VotedFor == -1 || sm.VotedFor == msg.CandidateID)
	canVoteYes := msg.LastTerm > sm.LastTerm || (msg.LastTerm == sm.LastTerm && msg.LastIndex >= sm.LastIndex)
	if canVote && canVoteYes {
		sm.Term = msg.Term
		sm.VotedFor = msg.CandidateID
		voteGranted = true
		sm.actionCh <- Alarm{AlarmTime: snoozeAlarmTime(ElectionTimeout)}
	} else {
		// reject vote
		voteGranted = false
	}
	sm.actionCh <- Send{msg.CandidateID, VoteResp{From: sm.ServerID, Term: sm.Term, VoteGranted: voteGranted}}
}

func (sm *StateMachine) onVoteResp(msg VoteResp) {
	//fmt.Println(sm.ServerID, "onVoteResp", msg)
	if sm.Term < msg.Term {
		sm.stepDown(msg.Term)
		sm.actionCh <- Alarm{AlarmTime: snoozeAlarmTime(ElectionTimeout)}
	}

	if sm.State == "Candidate" {
		if sm.Term == msg.Term {
			sm.VoteGranted[msg.From] = msg.VoteGranted
		}
		if sm.countVotes() > NumServers/2 {
			sm.State = "Leader"
			sm.LeaderID = sm.ServerID
			sm.NextIndex = make(map[int]int)
			sm.MatchIndex = make(map[int]int)
			for _, peer := range sm.PeerList {
				sm.NextIndex[peer] = len(sm.Log)
				sm.MatchIndex[peer] = -1
				prevLogIndex := sm.NextIndex[peer] - 1
				prevLogTerm := sm.getLogTerm(prevLogIndex)
				//emptyEntry := LogEntry{Data: []byte{}, Term: sm.Term}
				req := AppendEntriesReq{Term: sm.Term, LeaderID: sm.ServerID,
					PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
					Entries: nil, LeaderCommit: sm.CommitIndex}
				sm.actionCh <- Send{peer, req}
			}
			sm.actionCh <- Alarm{AlarmTime: snoozeAlarmTime(ElectionTimeout)}
		}
	}
}

func (sm *StateMachine) onTimeout() {
	//fmt.Println(sm.ServerID, "onTimeout")
	switch sm.State {
	case "Leader":
		for _, peer := range sm.PeerList {
			prevLogIndex := sm.NextIndex[peer] - 1
			prevLogTerm := sm.getLogTerm(prevLogIndex)
			// Heartbeat will be empty if sm.NextIndex[peer] == len(sm.Log)
			msg := Send{peer, AppendEntriesReq{LeaderID: sm.ServerID, Term: sm.Term,
				PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
				Entries: sm.Log[prevLogIndex+1 : len(sm.Log)], LeaderCommit: sm.CommitIndex}}
			sm.actionCh <- msg
		}
		sm.actionCh <- Alarm{AlarmTime: snoozeAlarmTime(HeartbeatTimeout)}
	case "Candidate", "Follower":
		sm.State = "Candidate"
		sm.Term++
		sm.VoteGranted = map[int]bool{sm.ServerID: true}
		sm.VotedFor = sm.ServerID
		for _, peer := range sm.PeerList {
			sm.actionCh <- Send{peer, VoteReq{Term: sm.Term, CandidateID: sm.ServerID,
				LastIndex: sm.LastIndex, LastTerm: sm.LastTerm}}
		}
		sm.actionCh <- Alarm{AlarmTime: snoozeAlarmTime(ElectionTimeout)}
	}
}

func (sm *StateMachine) onAppend(msg AppendMsg) {
	switch sm.State {
	case "Leader":
		// append data to leader's local log and don't send to followers. let the append go to the followers through heartbeat
		sm.LastIndex++
		entry := LogEntry{Data: msg.Data, Term: sm.Term}
		sm.Log = append(sm.Log, entry)
		sm.actionCh <- LogStore{Entry: entry, Index: sm.LastIndex}
	case "Follower", "Candidate":
		sm.actionCh <- CommitInfo{Index: -1, Data: msg.Data, Err: err_redirect}
	}
}

func (sm *StateMachine) eventLoop() {
	//if sm.State != "Leader" {
	//	fmt.Println("State: ", sm.State, "Index: ", sm.LastIndex, "Term: ", sm.Term)
	//}
	select {
	case appendMsg := <-sm.clientCh:
		msg := appendMsg.(AppendMsg)
		sm.onAppend(msg)
	case peerMsg := <-sm.netCh:
		t := reflect.TypeOf(peerMsg)
		switch t.Name() {
		case "AppendEntriesReq":
			sm.onAppendEntriesReq(peerMsg.(AppendEntriesReq))
		case "AppendEntriesResp":
			sm.onAppendEntriesResp(peerMsg.(AppendEntriesResp))
		case "VoteResp":
			sm.onVoteResp(peerMsg.(VoteResp))
		case "VoteReq":
			sm.onVoteReq(peerMsg.(VoteReq))
		case "Timeout":
			sm.onTimeout()
		}
		//TODO: write handler code for Alarm, LogStore messages
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
			clientCh:    make(chan interface{}, 1000),
			netCh:       make(chan interface{}, 1000),
			actionCh:    make(chan interface{}, 1000),
			VotedFor:    -1,
			CommitIndex: -1,
		}
		return &sm, nil
	}
	return &StateMachine{}, errors.New("Invalid state parameter")
}

// NewStateMachineBoot creates/restores a Raft state machine from a given config and log
func NewStateMachineBoot(conf *Config, log *log.Log) (*StateMachine, error) {
	sm := StateMachine{
		ServerID:    conf.Id,
		State:       "Follower",
		VoteGranted: nil,
		NextIndex:   nil,
		MatchIndex:  nil,
		clientCh:    make(chan interface{}, 1000),
		netCh:       make(chan interface{}, 1000),
		actionCh:    make(chan interface{}, 1000),
		VotedFor:    -1,
		CommitIndex: -1,
		LeaderID:    -1,
	}
	sm.PeerList = make([]int, len(conf.Peers))
	for i, peer := range conf.Peers {
		sm.PeerList[i] = peer.Id
	}

	//sm.Log = make([]LogEntry, 200)
	size := int(log.GetLastIndex()) + 1
	sm.Log = make([]LogEntry, size)
	for i := 0; i < size; i++ {
		data, _ := log.Get(int64(i))
		entry := data.(LogEntry)
		sm.Log[i] = entry
	}
	sm.LastIndex = size - 1
	sm.LastTerm = sm.getLogTerm(size - 1)
	return &sm, nil
}
