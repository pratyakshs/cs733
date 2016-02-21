package raft

import (
	"reflect"
	"testing"
)

func expectStepDownChanges(t *testing.T, sm *StateMachine, newTerm int, votedFor int) {
	switch {
	case sm.Term != newTerm:
		t.Fatalf("Expected term '%d', got '%d'", newTerm, sm.Term)
	case sm.VotedFor != votedFor:
		t.Fatalf("Expected VotedFor '%d', got '%d'", votedFor, sm.VotedFor)
	case sm.NextIndex != nil:
		t.Fatalf("Expected NextIndex nil, got '%s'", sm.NextIndex)
	case sm.MatchIndex != nil:
		t.Fatalf("Expected MatchIndex nil, got '%s'", sm.MatchIndex)
	case sm.State != "Follower":
		t.Fatalf("Expected State Follower, got '%s'", sm.State)
	case sm.VoteGranted != nil:
		t.Fatalf("Expected VoteGranted nil, got '%s'", sm.VoteGranted)
	}
}

func TestNewStateMachine_Success(t *testing.T) {
	states := []string{"Follower", "Candidate", "Leader"}
	term := 1
	leaderID := 2
	serverID := 3
	for _, state := range states {
		sm, err := NewStateMachine(term, leaderID, serverID, state)
		if err != nil {
			t.Fatalf("Expected no StateMachine errors, got '%s'", err)
		}
		switch {
		case sm.Term != term:
			t.Fatalf("Expected term '%d', got '%d'", term, sm.Term)
		case sm.LeaderID != leaderID:
			t.Fatalf("Expected leaderID '%d', got '%d'", leaderID, sm.LeaderID)
		case sm.ServerID != serverID:
			t.Fatalf("Expected serverID '%d', got '%d'", serverID, sm.ServerID)
		case sm.State != state:
			t.Fatalf("Expected state '%s', got '%s'", state, sm.State)
		}
	}
}

func TestNewStateMachine_Failure(t *testing.T) {
	_, err := NewStateMachine(1, 2, 3, "Foobar")
	if err == nil {
		t.Fatalf("Expected error, got none")
	}
}

func TestAppendEntriesReq_StepDown(t *testing.T) {
	sm, _ := NewStateMachine(1, 2, 3, "Leader")
	sm.VotedFor = 9
	msg := AppendEntriesReq{Term: 2, LeaderID: 5}
	sm.netCh <- msg
	sm.eventLoop()
	<-sm.actionCh

	// Assert state variable changes due to step down
	expectStepDownChanges(t, sm, 2, -1)
}

func TestAppendEntriesReq_Failure(t *testing.T) {
	sm, _ := NewStateMachine(5, 2, 3, "Follower")
	msg := AppendEntriesReq{Term: 2, LeaderID: 3}
	sm.netCh <- msg
	sm.eventLoop()
	respMsg := <-sm.actionCh
	respType := reflect.TypeOf(respMsg).Name()
	if respType != "Send" {
		t.Fatalf("Expected response type Send, got '%s'", respType)
	}
	respMsg = respMsg.(Send).Message
	respType = reflect.TypeOf(respMsg).Name()
	if respType != "AppendEntriesResp" {
		t.Fatalf("Expected response type AppendEntriesResp, got '%s'", respType)
	}

	response := respMsg.(AppendEntriesResp)
	switch {
	case response.From != 3:
		t.Fatalf("Expected response from '%d', got from '%d'", 3, response.From)
	case response.MatchIndex != -1:
		t.Fatalf("Expected MatchIndex '%d', got '%d'", -1, response.MatchIndex)
	case response.Term != 5:
		t.Fatalf("Expected Term '%d', got '%d'", 5, response.Term)
	case response.Success:
		t.Fatalf("Expected success false, got true")
	}
}

func TestAppendEntriesReq_Success1(t *testing.T) {
	sm, _ := NewStateMachine(5, 2, 3, "Follower")
	sm.LastIndex = 10

	logEntry1 := LogEntry{Data: []byte("foo")}
	logEntry2 := LogEntry{Data: []byte("bar")}
	logEntries := []LogEntry{logEntry1, logEntry2}
	msg := AppendEntriesReq{Term: 5, LeaderID: 3, PrevLogIndex: -1, Entries: logEntries, LeaderCommit: 0}
	sm.netCh <- msg
	sm.eventLoop()

	alarmAction := <-sm.actionCh
	action1 := <-sm.actionCh
	action2 := <-sm.actionCh
	alarmActionType := reflect.TypeOf(alarmAction).Name()
	action1Type := reflect.TypeOf(action1).Name()
	action2Type := reflect.TypeOf(action2).Name()

	// Assertions about type of actions
	switch {
	case alarmActionType != "Alarm":
		t.Fatalf("Expected first action of type Alarm, got '%s", alarmActionType)
	case action1Type != "LogStore":
		t.Fatalf("Expected first action of type LogStore, got '%s", action1Type)
	case action2Type != "LogStore":
		t.Fatalf("Expected second action of type LogStore, got '%s", action2Type)
	}

	// Assertions about first LogStore action
	logStore1 := action1.(LogStore)
	switch {
	case logStore1.From != 3:
		t.Fatalf("Expected response from '%d', got from '%d'", 3, logStore1.From)
	case logStore1.Index != 0:
		t.Fatalf("Expected LogStore index '%d', got '%d'", 0, logStore1.Index)
	case string(logStore1.Data) != "foo":
		t.Fatalf("Expected LogStore data 'foo', got '%s'", string(logStore1.Data))
	}

	// Assertions about second LogStore action
	logStore2 := action2.(LogStore)
	switch {
	case logStore2.From != 3:
		t.Fatalf("Expected response from '%d', got from '%d'", 3, logStore2.From)
	case logStore2.Index != 1:
		t.Fatalf("Expected LogStore index '%d', got '%d'", 1, logStore2.Index)
	case string(logStore2.Data) != "bar":
		t.Fatalf("Expected LogStore data 'bar', got '%s'", string(logStore2.Data))
	}

	respMsg := <-sm.actionCh
	respType := reflect.TypeOf(respMsg).Name()
	if respType != "Send" {
		t.Fatalf("Expected response type Send, got '%s'", respType)
	}
	respMsg = respMsg.(Send).Message
	respType = reflect.TypeOf(respMsg).Name()

	// Assertions about append entries response
	if respType != "AppendEntriesResp" {
		t.Fatalf("Expected response type AppendEntriesResp, got '%s'", respType)
	}

	response := respMsg.(AppendEntriesResp)
	switch {
	case response.From != 3:
		t.Fatalf("Expected response from '%d', got from '%d'", 3, response.From)
	case response.MatchIndex != 1:
		t.Fatalf("Expected MatchIndex '%d', got '%d'", 1, response.MatchIndex)
	case response.Term != 5:
		t.Fatalf("Expected Term '%d', got '%d'", 5, response.Term)
	case response.Success != true:
		t.Fatalf("Expected success true, got false")
	}

	// Assertions about update of state variables
	switch {
	case sm.LastIndex != 1:
		t.Fatalf("Expected LastIndex '%d', got '%d'", 1, sm.LastIndex)
	case sm.LastTerm != 5:
		t.Fatalf("Expected LastTerm '%d', got '%d'", 5, sm.LastTerm)
	case sm.CommitIndex != 0:
		t.Fatalf("Expected CommitIndex '%d', got '%d'", 0, sm.CommitIndex)
	}
}

func TestVoteReq_StepDown(t *testing.T) {
	sm, _ := NewStateMachine(1, 2, 3, "Leader")

	msg := VoteReq{Term: 2, CandidateID: 5}
	sm.netCh <- msg
	sm.eventLoop()

	<-sm.actionCh

	// Assert state variable changes due to step down
	// Vote is granted to candidate with ID 5
	expectStepDownChanges(t, sm, 2, 5)
}

func TestVoteReq_Failure(t *testing.T) {
	sm, _ := NewStateMachine(4, 3, 2, "Follower")
	sm.LastTerm = 4

	msg := VoteReq{Term: 4, LastTerm: 3}
	sm.netCh <- msg
	sm.eventLoop()

	respMsg := <-sm.actionCh
	respType := reflect.TypeOf(respMsg).Name()
	if respType != "Send" {
		t.Fatalf("Expected response type Send, got '%s'", respType)
	}
	respMsg = respMsg.(Send).Message
	respType = reflect.TypeOf(respMsg).Name()

	// Assertions about the response
	if respType != "VoteResp" {
		t.Fatalf("Expected response type VoteResp, got '%s'", respType)
	}

	response := respMsg.(VoteResp)
	switch {
	case response.From != 2:
		t.Fatalf("Expected response from '%d', got from '%d'", 2, response.From)
	case response.Term != 4:
		t.Fatalf("Expected Term '%d', got '%d'", 4, response.Term)
	case response.VoteGranted:
		t.Fatalf("Expected vote not granted, got vote granted instead")
	}
}

// Test success due to msg.LastTerm > sm.LastTerm
func TestVoteReq_Success1(t *testing.T) {
	sm, _ := NewStateMachine(4, 3, 2, "Follower")
	sm.VotedFor = -1
	sm.LastTerm = 3

	msg := VoteReq{Term: 4, LastTerm: 4}
	sm.netCh <- msg
	sm.eventLoop()

	alarmAction := <-sm.actionCh
	respMsg := <-sm.actionCh
	alarmActionType := reflect.TypeOf(alarmAction).Name()
	respType := reflect.TypeOf(respMsg).Name()
	if respType != "Send" {
		t.Fatalf("Expected response type Send, got '%s'", respType)
	}
	respMsg = respMsg.(Send).Message
	respType = reflect.TypeOf(respMsg).Name()

	// Assertions about the response types
	switch {
	case alarmActionType != "Alarm":
		t.Fatalf("Expected action of type Alarm, got '%s", alarmActionType)
	case respType != "VoteResp":
		t.Fatalf("Expected response type VoteResp, got '%s'", respType)
	}

	// Assertions about the vote response
	response := respMsg.(VoteResp)
	switch {
	case response.From != 2:
		t.Fatalf("Expected response from '%d', got from '%d'", 2, response.From)
	case response.Term != 4:
		t.Fatalf("Expected Term '%d', got '%d'", 4, response.Term)
	case response.VoteGranted != true:
		t.Fatalf("Expected vote granted, got vote not granted instead")
	}
}

// Test success due to msg.LastTerm == sm.LastTerm and msg.LastIndex >= sm.LastIndex
func TestVoteReq_Success2(t *testing.T) {
	sm, _ := NewStateMachine(4, 3, 2, "Follower")
	sm.VotedFor = -1
	sm.LastTerm = 4
	sm.LastIndex = 9

	msg := VoteReq{Term: 4, LastTerm: 4, LastIndex: 9}
	sm.netCh <- msg
	sm.eventLoop()

	alarmAction := <-sm.actionCh
	respMsg := <-sm.actionCh
	alarmActionType := reflect.TypeOf(alarmAction).Name()
	respType := reflect.TypeOf(respMsg).Name()
	if respType != "Send" {
		t.Fatalf("Expected response type Send, got '%s'", respType)
	}
	respMsg = respMsg.(Send).Message
	respType = reflect.TypeOf(respMsg).Name()

	// Assertions about the response types
	switch {
	case alarmActionType != "Alarm":
		t.Fatalf("Expected action of type Alarm, got '%s", alarmActionType)
	case respType != "VoteResp":
		t.Fatalf("Expected response type VoteResp, got '%s'", respType)
	}

	// Assertions about the vote response
	response := respMsg.(VoteResp)
	switch {
	case response.From != 2:
		t.Fatalf("Expected response from '%d', got from '%d'", 2, response.From)
	case response.Term != 4:
		t.Fatalf("Expected Term '%d', got '%d'", 4, response.Term)
	case response.VoteGranted != true:
		t.Fatalf("Expected vote granted, got vote not granted instead")
	}
}

func TestVoteResp_StepDown(t *testing.T) {
	sm, _ := NewStateMachine(1, 2, 3, "Candidate")

	msg := VoteResp{Term: 2}
	sm.netCh <- msg
	sm.eventLoop()

	alarmAction := <-sm.actionCh

	// Assert state variable changes due to step down
	expectStepDownChanges(t, sm, 2, -1)

	alarmActionType := reflect.TypeOf(alarmAction).Name()
	if alarmActionType != "Alarm" {
		t.Fatalf("Expected action of type Alarm, got '%s", alarmActionType)
	}
}

func TestVoteResp_VoteNotGranted(t *testing.T) {
	sm, _ := NewStateMachine(1, 2, 3, "Candidate")
	sm.VoteGranted = make(map[int]bool)

	msg := VoteResp{From: 5, Term: 1, VoteGranted: false}
	sm.netCh <- msg
	sm.eventLoop()

	// Assert vote stored properly
	val, ok := sm.VoteGranted[5]
	switch {
	case !ok:
		t.Fatalf("Expected vote from server '%d' to be recored, not recorded instead", 5)
	case val:
		t.Fatalf("Expected vote from server '%d' not granted, was granted instead", 5)
	}
}

func TestVoteResp_LeaderElected(t *testing.T) {
	sm, _ := NewStateMachine(1, -1, 3, "Candidate")
	sm.VoteGranted = make(map[int]bool)
	sm.VoteGranted[2] = true
	sm.VoteGranted[3] = true
	sm.PeerList = []int{1, 2, 4, 5}

	msg := VoteResp{From: 5, Term: 1, VoteGranted: true}
	sm.netCh <- msg
	sm.eventLoop()

	// Assert vote stored properly
	val, ok := sm.VoteGranted[5]
	switch {
	case !ok:
		t.Fatalf("Expected vote from server '%d' to be recored, not recorded instead", 5)
	case !val:
		t.Fatalf("Expected vote from server '%d' granted, was not granted instead", 5)
	}

	// Assert state change to Leader
	switch {
	case sm.State != "Leader":
		t.Fatalf("Expected State Leader, got '%s'", sm.State)
	case sm.LeaderID != 3:
		t.Fatalf("Expected LeaderID '%d', got '%d'", 3, sm.LeaderID)
	case sm.NextIndex == nil:
		t.Fatalf("Expected NextIndex not nil, got nil")
	case sm.MatchIndex == nil:
		t.Fatalf("Expected MatchIndex not nil, got nil")
	}

	emptyEntry := LogEntry{Data: []byte{}, Term: sm.Term}
	// Assert NextIndex, MatchIndex initialized properly and heartbeats sent
	for _, peer := range sm.PeerList {
		switch {
		case sm.NextIndex[peer] != len(sm.Log):
			t.Fatalf("Expected NextIndex['%d'] = '%d', got '%d'", peer, len(sm.Log), sm.NextIndex[peer])
		case sm.MatchIndex[peer] != -1:
			t.Fatalf("Expected MatchIndex['%d'] = '%d', got '%d'", peer, -1, sm.MatchIndex[peer])
		}
		hbMsg := <-sm.actionCh
		msgType := reflect.TypeOf(hbMsg).Name()

		if msgType != "Send" {
			t.Fatalf("Expected response type Send, got '%s'", msgType)
		}

		dest := hbMsg.(Send).To
		hbMsg = hbMsg.(Send).Message
		msgType = reflect.TypeOf(hbMsg).Name()
		switch {
		case dest != peer:
			t.Fatalf("Expected message destination '%d', got '%d'", peer, dest)
		case msgType != "AppendEntriesReq":
			t.Fatalf("Expected message type AppendEntriesReq, got '%s'", msgType)
		}

		// Assert heartbeat attributes correct
		hb := hbMsg.(AppendEntriesReq)
		switch {
		case hb.Term != 1:
			t.Fatalf("Expected Term '%d', got '%d'", 1, hb.Term)
		case hb.LeaderID != 3:
			t.Fatalf("Expected LeaderID '%d', got '%d'", 3, hb.LeaderID)
		case hb.PrevLogIndex != len(sm.Log)-1:
			t.Fatalf("Expected PrevLogIndex '%d', got '%d'", len(sm.Log)-1, hb.PrevLogIndex)
		case hb.PrevLogTerm != sm.getLogTerm(hb.PrevLogIndex):
			t.Fatalf("Expected PrevLogTerm '%d', got '%d'", sm.getLogTerm(hb.PrevLogIndex), hb.PrevLogTerm)
		case len(hb.Entries) != 1:
			t.Fatalf("Expected '%d' entries, got '%d'", 1, len(hb.Entries))
		case string(hb.Entries[0].Data) != "":
			t.Fatalf("Expected entry: '%s', got '%s'", emptyEntry, hb.Entries[0])
		case hb.LeaderCommit != sm.CommitIndex:
			t.Fatalf("Expected LeaderCommit '%d', got '%d'", sm.CommitIndex, hb.LeaderCommit)
		}
	}
}

func TestAppendEntriesResp_StepDown(t *testing.T) {
	sm, _ := NewStateMachine(1, 2, 3, "Leader")

	msg := AppendEntriesResp{Term: 2}
	sm.netCh <- msg
	sm.eventLoop()

	alarmAction := <-sm.actionCh

	// Assert state variable changes due to step down
	expectStepDownChanges(t, sm, 2, -1)

	alarmActionType := reflect.TypeOf(alarmAction).Name()
	if alarmActionType != "Alarm" {
		t.Fatalf("Expected action of type Alarm, got '%s", alarmActionType)
	}
}

func TestAppendEntriesResp_Success(t *testing.T) {
	sm, _ := NewStateMachine(1, 2, 3, "Leader")
	sm.Log = []LogEntry{LogEntry{[]byte("foo"), 0}, LogEntry{[]byte("bar"), 1}}
	sm.NextIndex = make(map[int]int)
	sm.MatchIndex = map[int]int{1: 1, 2: 1, 4: 0, 5: -1}

	msg := AppendEntriesResp{Term: 1, Success: true, MatchIndex: 0, From: 5}
	sm.netCh <- msg
	sm.eventLoop()

	// Assertions about volatile state update
	switch {
	case sm.MatchIndex[5] != 0:
		t.Fatalf("Expected MatchIndex['%d']='%d', got '%d'", 5, 0, sm.MatchIndex[5])
	case sm.NextIndex[5] != 1:
		t.Fatalf("Expected NextIndex['%d']='%d', got '%d'", 5, 1, sm.NextIndex[5])
	case sm.CommitIndex != 1:
		t.Fatalf("Expected CommitIndex '%d', got '%d'", 1, sm.CommitIndex)
	}

	respMsg := <-sm.actionCh
	respType := reflect.TypeOf(respMsg).Name()
	if respType != "Send" {
		t.Fatalf("Expected response type Send, got '%s'", respType)
	}
	dest := respMsg.(Send).To
	respMsg = respMsg.(Send).Message
	respType = reflect.TypeOf(respMsg).Name()

	// Assertions about the response
	switch {
	case dest != 5:
		t.Fatalf("Expected message destination '%d', got '%d'", 5, dest)
	case respType != "AppendEntriesReq":
		t.Fatalf("Expected response type AppendEntriesReq, got '%s'", respType)
	}

	response := respMsg.(AppendEntriesReq)
	switch {
	case response.Term != 1:
		t.Fatalf("Expected Term '%d', got '%d'", 1, response.Term)
	case response.LeaderID != 3:
		t.Fatalf("Expected LeaderID '%d', got '%d'", 3, response.LeaderID)
	case response.PrevLogIndex != 0:
		t.Fatalf("Expected PrevLogIndex '%d', got '%d'", 0, response.PrevLogIndex)
	case response.PrevLogTerm != 0:
		t.Fatalf("Expected PrevLogTerm '%d', got '%d'", 0, response.PrevLogTerm)
	case len(response.Entries) != 1:
		t.Fatalf("Expected '%d' entries, got '%d'", 1, len(response.Entries))
	case string(response.Entries[0].Data) != "bar":
		t.Fatalf("Expected entry: '%s', got '%s'", "bar", response.Entries[0].Data)
	case response.LeaderCommit != sm.CommitIndex:
		t.Fatalf("Expected LeaderCommit '%d', got '%d'", sm.CommitIndex, response.LeaderCommit)
	}
}

// NextIndex[] = 0
func TestAppendEntriesResp_Failure1(t *testing.T) {
	sm, _ := NewStateMachine(1, 2, 3, "Leader")
	sm.NextIndex = make(map[int]int)
	sm.NextIndex[5] = 0
	sm.Log = []LogEntry{LogEntry{[]byte("foo"), 0}}

	msg := AppendEntriesResp{From: 5, Term: 1, Success: false}
	sm.netCh <- msg
	sm.eventLoop()

	if sm.NextIndex[5] != 0 {
		t.Fatalf("Expected NextIndex['%d'] = '%d', got '%d", 5, 0, sm.NextIndex[5])
	}

	respMsg := <-sm.actionCh
	respType := reflect.TypeOf(respMsg).Name()
	if respType != "Send" {
		t.Fatalf("Expected response type Send, got '%s'", respType)
	}
	dest := respMsg.(Send).To
	respMsg = respMsg.(Send).Message
	respType = reflect.TypeOf(respMsg).Name()

	// Assertions about the response
	switch {
	case dest != 5:
		t.Fatalf("Expected message destination '%d', got '%d'", 5, dest)
	case respType != "AppendEntriesReq":
		t.Fatalf("Expected response type AppendEntriesReq, got '%s'", respType)
	}

	response := respMsg.(AppendEntriesReq)
	switch {
	case response.Term != 1:
		t.Fatalf("Expected Term '%d', got '%d'", 1, response.Term)
	case response.LeaderID != 3:
		t.Fatalf("Expected LeaderID '%d', got '%d'", 3, response.LeaderID)
	case response.PrevLogIndex != -1:
		t.Fatalf("Expected PrevLogIndex '%d', got '%d'", -1, response.PrevLogIndex)
	case response.PrevLogTerm != -1:
		t.Fatalf("Expected PrevLogTerm '%d', got '%d'", -1, response.PrevLogTerm)
	case len(response.Entries) != 1:
		t.Fatalf("Expected '%d' entries, got '%d'", 1, len(response.Entries))
	case string(response.Entries[0].Data) != "foo":
		t.Fatalf("Expected entry: '%s', got '%s'", "foo", response.Entries[0].Data)
	case response.LeaderCommit != sm.CommitIndex:
		t.Fatalf("Expected LeaderCommit '%d', got '%d'", sm.CommitIndex, response.LeaderCommit)
	}
}

func TestLeaderTimeout(t *testing.T) {
	sm, _ := NewStateMachine(1, 2, 3, "Leader")
	sm.PeerList = []int{1, 2, 4, 5}
	sm.NextIndex = map[int]int{1: 0, 2: 1, 4: 1, 5: 0}
	sm.Log = []LogEntry{LogEntry{[]byte("foo"), 0}, LogEntry{[]byte("bar"), 1}}

	sm.netCh <- Timeout{}
	sm.eventLoop()

	for _, peer := range sm.PeerList {
		respMsg := <-sm.actionCh
		respType := reflect.TypeOf(respMsg).Name()
		if respType != "Send" {
			t.Fatalf("Expected response type Send, got '%s'", respType)
		}
		dest := respMsg.(Send).To
		respMsg = respMsg.(Send).Message
		respType = reflect.TypeOf(respMsg).Name()

		// Assertions about the response
		switch {
		case dest != peer:
			t.Fatalf("Expected message destination '%d', got '%d'", peer, dest)
		case respType != "AppendEntriesReq":
			t.Fatalf("Expected response type AppendEntriesReq, got '%s'", respType)
		}

		response := respMsg.(AppendEntriesReq)
		prevLogIndex := sm.NextIndex[peer] - 1
		prevLogTerm := sm.getLogTerm(prevLogIndex)
		numEntries := len(sm.Log) - sm.NextIndex[peer]
		switch {
		case response.Term != 1:
			t.Fatalf("Expected Term '%d', got '%d'", 1, response.Term)
		case response.LeaderID != 3:
			t.Fatalf("Expected LeaderID '%d', got '%d'", 3, response.LeaderID)
		case response.PrevLogIndex != prevLogIndex:
			t.Fatalf("Expected PrevLogIndex '%d', got '%d'", prevLogIndex, response.PrevLogIndex)
		case response.PrevLogTerm != prevLogTerm:
			t.Fatalf("Expected PrevLogTerm '%d', got '%d'", prevLogTerm, response.PrevLogTerm)
		case len(response.Entries) != numEntries:
			t.Fatalf("Expected '%d' entries, got '%d'", numEntries, len(response.Entries))
			//TODO: check contents of entries
		//case string(response.Entries[0].Data) != "foo":
		//	t.Fatalf("Expected entry: '%s', got '%s'", "foo", response.Entries[0].Data)
		case response.LeaderCommit != sm.CommitIndex:
			t.Fatalf("Expected LeaderCommit '%d', got '%d'", sm.CommitIndex, response.LeaderCommit)
		}
	}

	alarmAction := <-sm.actionCh
	alarmActionType := reflect.TypeOf(alarmAction).Name()
	if alarmActionType != "Alarm" {
		t.Fatalf("Expected action of type Alarm, got '%s", alarmActionType)
	}
}

func TimeoutTest(t *testing.T, state string) {
	sm, _ := NewStateMachine(1, 2, 3, state)
	sm.PeerList = []int{1, 2, 4, 5}
	sm.LastIndex = 5
	sm.LastTerm = 1

	sm.netCh <- Timeout{}
	sm.eventLoop()

	switch {
	case sm.State != "Candidate":
		t.Fatalf("Expected State 'Candidate', got '%s'", sm.State)
	case sm.Term != 2:
		t.Fatalf("Expected Term '%d', got '%d'", 2, sm.Term)
	case sm.VoteGranted[3] != true:
		t.Fatalf("Expected VoteGranted['%d'] = true, got false")
	case sm.countVotes() != 1:
		t.Fatalf("Expected vote count '%d', got '%d'", 1, sm.countVotes())
	case sm.VotedFor != 3:
		t.Fatalf("Expected VotedFor '%d', got '%d'", 3, sm.VotedFor)
	}
	for _, peer := range sm.PeerList {
		reqMsg := <-sm.actionCh
		reqType := reflect.TypeOf(reqMsg).Name()
		if reqType != "Send" {
			t.Fatalf("Expected response type Send, got '%s'", reqType)
		}
		dest := reqMsg.(Send).To
		reqMsg = reqMsg.(Send).Message
		reqType = reflect.TypeOf(reqMsg).Name()

		// Assertions about the vote request
		switch {
		case dest != peer:
			t.Fatalf("Expected message destination '%d', got '%d'", peer, dest)
		case reqType != "VoteReq":
			t.Fatalf("Expected response type VoteReq, got '%s'", reqType)
		}

		request := reqMsg.(VoteReq)
		switch {
		case request.Term != 2:
			t.Fatalf("Expected Term '%d', got '%d'", 2, request.Term)
		case request.CandidateID != 3:
			t.Fatalf("Expected CandidateID '%d', got '%d'", 3, request.CandidateID)
		case request.LastIndex != 5:
			t.Fatalf("Expected LastIndex '%d', got '%d'", 5, request.LastIndex)
		case request.LastTerm != 1:
			t.Fatalf("Expected LastTerm '%d', got '%d'", 1, request.LastTerm)
		}
	}

	alarmAction := <-sm.actionCh
	alarmActionType := reflect.TypeOf(alarmAction).Name()
	if alarmActionType != "Alarm" {
		t.Fatalf("Expected action of type Alarm, got '%s", alarmActionType)
	}
}

func TestElectionTimeout(t *testing.T) {
	TimeoutTest(t, "Follower")
	TimeoutTest(t, "Candidate")
}
