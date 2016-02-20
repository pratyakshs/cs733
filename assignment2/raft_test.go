package raft

import (
	"reflect"
	"testing"
)

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
	go sm.eventLoop()
	msg := AppendEntriesReq{Term: 2, LeaderID: 5}
	sm.netCh <- msg
	respMsg := <-sm.actionCh
	respType := reflect.TypeOf(respMsg).Name()

	switch {
	case sm.Term != 2:
		t.Fatalf("Expected term '%d', got '%d'", 2, sm.Term)
	case sm.VotedFor != -1:
		t.Fatalf("Expected VotedFor '%d', got '%d'", -1, sm.VotedFor)
	case sm.NextIndex != nil:
		t.Fatalf("Expected NextIndex nil, got '%s'", sm.NextIndex)
	case sm.MatchIndex != nil:
		t.Fatalf("Expected MatchIndex nil, got '%s'", sm.MatchIndex)
	case sm.State != "Follower":
		t.Fatalf("Expected State Follower, got '%s'", sm.State)
	case sm.VoteGranted != nil:
		t.Fatalf("Expected VoteGranted nil, got '%s'", sm.VoteGranted)
	case respType != "AppendEntriesResp":
		t.Fatalf("Expected response type AppendEntriesResp, got '%s'", respType)
	}

	response := respMsg.(AppendEntriesResp)
	switch {
	case response.From != 3:
		t.Fatalf("Expected response from '%d', got from '%d'", 3, response.From)
	case response.Term != 2:
		t.Fatalf("Expected Term '%d', got '%d'", 2, response.Term)
	case response.MatchIndex != -1:
		t.Fatalf("Expected MatchIndex '%d', got '%d'", -1, response.MatchIndex)
	case response.Success != true:
		t.Fatalf("Expected success true, got false")
	}
}

func TestAppendEntriesReq_Failure(t *testing.T) {
	sm, _ := NewStateMachine(5, 2, 3, "Follower")
	go sm.eventLoop()
	msg := AppendEntriesReq{Term: 2, LeaderID: 3}
	sm.netCh <- msg
	respMsg := <-sm.actionCh
	respType := reflect.TypeOf(respMsg).Name()

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

// TODO: Give better name
func TestAppendEntriesReq_Success1(t *testing.T) {
	sm, _ := NewStateMachine(5, 2, 3, "Follower")
	sm.LastIndex = 10
	go sm.eventLoop()

	logEntry1 := LogEntry{Data: []byte("foo"), Committed: true, Term: 5, LogIndex: 0}
	logEntry2 := LogEntry{Data: []byte("bar"), Committed: false, Term: 5, LogIndex: 1}
	logEntries := []LogEntry{logEntry1, logEntry2}
	msg := AppendEntriesReq{Term: 5, LeaderID: 3, PrevLogIndex: -1, Entries: logEntries, LeaderCommit: 0}
	sm.netCh <- msg
	action1 := <-sm.actionCh
	action2 := <-sm.actionCh
	action1Type := reflect.TypeOf(action1).Name()
	action2Type := reflect.TypeOf(action2).Name()

	// Assertions about type of actions
	switch {
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
