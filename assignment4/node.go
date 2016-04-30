package main

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/log"
	"github.com/syndtr/goleveldb/leveldb"
)

var ROOT_DIR = os.Getenv("GOPATH") + "src/github.com/pratyakshs/cs733/assignment4/"
var CONFIG_FILE = ROOT_DIR + "raft_config.json"
var LOG_DIR = ROOT_DIR + "logs/"
var DB_DIR = ROOT_DIR + "db/"
var FS_DIR = ROOT_DIR + "fs/"

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

type PeerConfig struct {
	Id            int
	Address       string
	ClientAddress string
}

type NetConfig struct {
	Id   int
	Host string
	Port int
}

type Config struct {
	Peers      []PeerConfig // Information about all servers, including this.
	Id         int          // this node's id. One of the cluster's entries should match.
	LogFile    string       // Log file for this node
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
	node.clientCh <- AppendMsg{data}
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

//func ToClusterConfig(conf Config) cluster.Config {
//	return cluster.Config{Peers: conf.Peers.([]cluster.PeerConfig), OutboxSize: conf.OutboxSize, InboxSize:conf.InboxSize}
//}

func NewRaftNode(conf Config) (RaftNode, error) {
	var node RaftNode
	var err error
	node.log, err = log.Open(conf.LogFile)
	if err != nil {
		return node, err
	}
	node.sm, err = NewStateMachineBoot(&conf, node.log)
	if err != nil {
		return node, err
	}

	node.server, err = cluster.New(conf.Id, CONFIG_FILE)
	if err != nil {
		fmt.Println("Error in cluster")
	}

	node.config = conf

	node.commitCh = make(chan CommitInfo, 1000)
	node.clientCh = make(chan AppendMsg, 1000)

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

func GetConfig(id int) Config {

	return Config{
		InboxSize:  100,
		OutboxSize: 100,
		Peers: []PeerConfig{
			PeerConfig{Id: 0, Address: "localhost:8001", ClientAddress: "localhost:9001"},
			PeerConfig{Id: 1, Address: "localhost:8002", ClientAddress: "localhost:9002"},
			PeerConfig{Id: 2, Address: "localhost:8003", ClientAddress: "localhost:9003"},
			PeerConfig{Id: 3, Address: "localhost:8004", ClientAddress: "localhost:9004"},
			PeerConfig{Id: 4, Address: "localhost:8005", ClientAddress: "localhost:9005"},
		},
		LogFile: LOG_DIR + strconv.FormatInt(int64(id), 10),
		Id:      id,
	}

	//var cfg Config
	//var f *os.File
	//var err error
	//if f, err = os.Open(CONFIG_FILE); err != nil {
	//	return cfg, err
	//}
	//defer f.Close()
	//dec := json.NewDecoder(f)
	//if err = dec.Decode(&cfg); err != nil {
	//	return cfg, err
	//}
	//return cfg, nil

}

func makeRaftNodes() []RaftNode {
	var rafts []RaftNode
	conf := GetConfig(0)
	fmt.Println(len(conf.Peers))
	for i := 0; i < len(conf.Peers); i++ {
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
		node.timer = time.NewTimer(time.Duration(ElectionTimeout+rand.Intn(2000)) * time.Millisecond)
	}

	for !node.shutdown {
		select {
		case env := <-node.server.Inbox():
			msg := env.Msg
			node.sm.netCh <- msg
			node.sm.eventLoop()
		case appendMsg := <-node.clientCh:
			node.sm.clientCh <- appendMsg
			node.sm.eventLoop()
		case <-node.timer.C:
			node.sm.netCh <- Timeout{}
			node.sm.eventLoop()
		case action := <-node.sm.actionCh:
			t := reflect.TypeOf(action)
			switch t.Name() {
			case "Send":
				env, _ := action.(Send)
				node.server.Outbox() <- &cluster.Envelope{Pid: env.To, Msg: env.Message}
			case "Alarm":
				alarm, _ := action.(Alarm)
				node.timer.Reset(alarm.AlarmTime)
			case "LogStore":
				ac := action.(LogStore)
				node.LogStoreHandler(ac)
			case "CommitInfo":
				ac := action.(CommitInfo)
				node.CommitHandler(ac)
			}
		}
	}
}

func (node *RaftNode) CommitHandler(ac CommitInfo) {
	node.commitCh <- ac
}

func (node *RaftNode) LogStoreHandler(ac LogStore) {
	lastIndex := node.log.GetLastIndex()
	if int(lastIndex) >= ac.Index {
		node.log.TruncateToEnd(int64(ac.Index))
	} else {
		err := node.log.Append(ac.Entry)
		if err != nil {
			fmt.Println("log append failed! err = ", err)
		}
	}
}

//func New() RaftNode {
//
//}

//func RestartNode(i int, clusterconf []NetConfig) RaftNode {
//	ld := "myLogDir" + strconv.Itoa(i)
//	sd := "StateId_" + strconv.Itoa(i)
//	rn := NewRaftNode()
//	rc := RaftConfig{cluster: clusterconf, Id: i, LogDir: ld, StateFile: sd, ElectionTimeout: eo, HeartbeatTimeout: 600}
//	rs := New(rc)
//	return RaftNode{}
//
//}

//func BringNodeUp(i int, clusterconf []NetConfig) RaftNode {
//	//TODO: initialize state on disk as follower and restart node
//	_ = RestartNode(i, clusterconf)
//	return RaftNode{}
//}

func initRaft() {
	gob.Register(AppendEntriesReq{})
	gob.Register(AppendEntriesResp{})
	gob.Register(VoteReq{})
	gob.Register(VoteResp{})
	gob.Register(Timeout{})
	gob.Register(AppendMsg{})
	gob.Register(LogEntry{})
}
