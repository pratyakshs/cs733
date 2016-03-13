package raft

import (
	"encoding/gob"
	"fmt"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/log"
	"github.com/syndtr/goleveldb/leveldb"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"
)

var ROOT_DIR = os.Getenv("GOPATH") + "src/github.com/pratyakshs/cs733/assignment3/"
var CONFIG_FILE = ROOT_DIR + "raft_config.json"
var LOG_DIR = ROOT_DIR + "logs/"
var DB_DIR = ROOT_DIR + "db/"

type Node interface {
	// Client's message to Raft node
	Append([]byte)

	// A channel for client to listen on. What goes into Append must come out of here at some point.
	CommitChannel() <-chan CommitInfo

	// Last known committed index in the log. This could be -1 until the system stabilizes.
	CommittedIndex() int

	// Returns the data at a log index, or an error.
	Get(index int) (error, []byte)

	// Node's id
	Id()

	// Id of leader. -1 if unknown
	LeaderId() int

	// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
	Shutdown()
}

// data goes in via Append, comes out as CommitInfo from the node's CommitChannel
// Index is valid only if err == nil
type CommitInfo struct {
	Data  []byte
	Index int64 // or int .. whatever you have in your code
	Err   error // Err can be errred
}

type Config struct {
	cluster    []cluster.PeerConfig // Information about all servers, including this.
	Id         int                  // this node's id. One of the cluster's entries should match.
	LogFile    string               // Log file for this node
	InboxSize  int
	OutboxSize int
}

type RaftNode struct {
	sm       *StateMachine
	server   cluster.Server
	log      *log.Log
	config   Config
	clientCh chan AppendMsg
	commitCh chan CommitInfo
	mutex    *sync.RWMutex
	db       *leveldb.DB
	shutdown bool
	timer    *time.Timer
	//TODO: do we need any other channels
}

func (node *RaftNode) Append(data []byte) {
	node.sm.clientCh <- AppendMsg{data}
}

func (node *RaftNode) CommitChannel() <-chan CommitInfo {
	return node.commitCh
}

func (node *RaftNode) Get(index int64) ([]byte, error) {
	node.mutex.RLock()
	defer node.mutex.RUnlock()
	data, err := node.log.Get(index)
	entry := data.(LogEntry)
	return entry.Data, err
}

func (node *RaftNode) CommittedIndex() int {
	node.mutex.RLock()
	defer node.mutex.RUnlock()
	return node.sm.CommitIndex
}

func (node *RaftNode) Id() int {
	return node.sm.ServerID
}

func (node *RaftNode) LeaderId() int {
	node.mutex.RLock()
	defer node.mutex.RUnlock()
	return node.sm.LeaderID
}

func (node *RaftNode) ShutDown() {
	node.shutdown = true
	//TODO: close channels
	//TODO: close leveldb
	//TODO: Stop timers
	//TODO: log.Close()
	//TODO: node.server.Close()
}

func NewRaftNode(conf Config) (RaftNode, error) {
	var node RaftNode
	var err error

	node.log, err = log.Open(conf.LogFile)
	if err != nil {
		return node, err
	}
	fmt.Print("RaftCreated ")
	fmt.Println(conf.Id)
	node.sm, err = NewStateMachineBoot(&conf, node.log)
	if err != nil {
		return node, err
	}

	node.server, _ = cluster.New(conf.Id, CONFIG_FILE)

	node.config = conf

	node.commitCh = make(chan CommitInfo)

	//TODO: initialize other channels..
	node.mutex = &sync.RWMutex{}

	node.db, err = leveldb.OpenFile(DB_DIR+strconv.Itoa(conf.Id), nil)
	if err != nil {
		return node, err
	}
	defer node.db.Close()

	term, err := node.db.Get([]byte("term"), nil)
	if err != nil {
		node.sm.Term = 0
	} else {
		node.sm.Term, _ = strconv.Atoi(string(term))
	}

	votedFor, err := node.db.Get([]byte("votedFor"), nil)
	if err != nil {
		node.sm.VotedFor = -1
	} else {
		node.sm.VotedFor, _ = strconv.Atoi(string(votedFor))
	}
	node.shutdown = false

	return node, nil
}

func GetConfig() (Config, error) {
	cluster_config, err := cluster.ToConfig(CONFIG_FILE)
	var conf Config
	if err != nil {
		return conf, err
	}
	conf.cluster = cluster_config.Peers
	conf.InboxSize = cluster_config.InboxSize
	conf.OutboxSize = cluster_config.OutboxSize
	return conf, nil
}

func makeRaftNodes() []RaftNode {
	var rafts []RaftNode
	conf, err := GetConfig()
	if err != nil {

	}
	fmt.Println(len(conf.cluster))
	for i := 0; i < len(conf.cluster); i++ {
		conf.Id = i
		conf.LogFile = LOG_DIR + strconv.Itoa(i)
		node, _ := NewRaftNode(conf)
		rafts = append(rafts, node)
	}
	return rafts
}

func (node *RaftNode) eventLoop() {
	if node.sm.State == "Leader" {
		node.timer = time.NewTimer(time.Duration(HeartbeatTimeout+rand.Intn(20000)) * time.Millisecond)
	} else {
		fmt.Println("TimerSet")
		node.timer = time.NewTimer(time.Duration(ElectionTimeout+rand.Intn(2000)) * time.Millisecond)
	}

	for !node.shutdown {
		select {
		case env := <-node.server.Inbox():
			msg := env.Msg
			node.sm.netCh <- msg
			go node.sm.eventLoop()
		case appendMsg := <-node.clientCh:
			node.sm.clientCh <- appendMsg
			go node.sm.eventLoop()
		case <-node.timer.C:
			node.sm.netCh <- Timeout{}
			go node.sm.eventLoop()
		case action := <-node.sm.actionCh:
			t := reflect.TypeOf(action)
			if t.Name() == "Send" {
				env, _ := action.(Send)
				node.server.Outbox() <- &cluster.Envelope{Pid: env.To, Msg: env.Message}
			} else if t.Name() == "Alarm" {
				alarm, _ := action.(Alarm)
				node.timer.Reset(alarm.AlarmTime)
			}
		}
	}
}

func initRaft() {
	gob.Register(AppendEntriesReq{})
	gob.Register(AppendEntriesResp{})
	gob.Register(VoteReq{})
	gob.Register(VoteResp{})
	gob.Register(Timeout{})
	gob.Register(AppendMsg{})
}
