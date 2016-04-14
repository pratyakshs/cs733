package raft

import (
	"fmt"
	"github.com/cs733-iitb/cluster"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	initRaft()
	m.Run()
}

func TestBasic(t *testing.T) {
	rafts := makeRaftNodes() // array of []raft.Node
	//time.Sleep( * (time.Second))
	for i := 0; i < 5; i++ {
		go rafts[i].eventLoop()
	}
	time.Sleep(60 * (time.Second))
	for i, raft := range rafts {
		fmt.Println(i, raft.sm.State)
	}
	//fmt.Println(rafts[1].LeaderId())
	//fmt.Println("yo")
	//ldr.Append("foo")

	//for _, node:= rafts {
	//select { // to avoid blocking on channel.
	//case ci := <- node.CommitChannel():
	//	if ci.err != nil {t.Fatal(ci.err)}
	//	if string(ci.data) != "foo" {
	//		t.Fatal("Got different data")
	//	}
	//default: t.Fatal("Expected message on all nodes")
	//}

}

func TestGetConfig(t *testing.T) {
	conf, err := GetConfig()
	if err != nil {
		t.Fatalf("Received error '%s' from GetConfig()", err)
	}
	switch {
	case conf.InboxSize != 100:
		t.Fatalf("Expected InboxSize 100, got '%d'", conf.InboxSize)
	case conf.OutboxSize != 100:
		t.Fatalf("Expected OutboxSize 100, got '%d'", conf.OutboxSize)
	}
	cluster := []cluster.PeerConfig{
		{Id: 0, Address: "localhost:8001"},
		{Id: 1, Address: "localhost:8002"},
		{Id: 2, Address: "localhost:8003"},
		{Id: 3, Address: "localhost:8004"},
		{Id: 4, Address: "localhost:8005"}}
	if len(cluster) != len(conf.cluster) {
		t.Fatalf("Expected '%d' peers in config, got '%d'", len(cluster), len(conf.cluster))
	}
	for i, peer := range cluster {
		if peer != conf.cluster[i] {
			t.Fatalf("Expected peer '%s', got '%s'", peer, conf.cluster[i])
		}
	}
}
