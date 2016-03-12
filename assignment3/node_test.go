package raft

import (
	"testing"
	"time"
)

func TestTemp(t *testing.T) {

	//rafts := makeRafts()

}


func TestBasic (t *testing.T) {
	rafts := makeRafts() // array of []raft.Node
	ldr := getLeader(rafts)
	ldr.Append("foo")
	time.Sleep(1 (time.Second))

	for _, node:= rafts {
	select { // to avoid blocking on channel.
	case ci := <- node.CommitChannel():
		if ci.err != nil {t.Fatal(ci.err)}
		if string(ci.data) != "foo" {
			t.Fatal("Got different data")
		}
	default: t.Fatal("Expected message on all nodes")
	}
}
}

