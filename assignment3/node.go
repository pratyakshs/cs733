package raft

import (
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/log"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
	"strconv"
	"sync"
)

const ROOT_DIR = os.Getenv("GOPATH") + "src/github.com/pratyakshs/cs733/assignment3/"
const CONFIG_FILE = ROOT_DIR + "raft_config.json"
const LOG_DIR = ROOT_DIR + "logs/"
const DB_DIR = ROOT_DIR + "db/"

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
	commitCh chan CommitInfo
	mutex    *sync.RWMutex
	db       *leveldb.DB
	//TODO: do we need any other channels
}

func (node *RaftNode) Append(data []byte) {
	node.sm.clientCh <- AppendMsg{data}
}

func (node *RaftNode) CommitChannel() <-chan CommitInfo {
	return node.commitCh
}

func (node *RaftNode) Get(index int64) (error, []byte) {
	node.mutex.RLock()
	defer node.mutex.RUnlock()
	return node.sm.CommitIndex
}

func (node *RaftNode) CommittedIndex() <-chan CommitInfo {
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
	//TODO: close channels
	//TODO: close leveldb
	//TODO: Stop timers
	//TODO: log.Close()
	//TODO: node.server.Close()
}

//FIXME: for config - pointer or object?
func NewRaftNode(conf *Config) (Node, error) {

	var node RaftNode
	var err error

	node.sm = NewStateMachine(0, -1, conf.Id, "Follower")
	cluster_config, err := cluster.ToConfig(CONFIG_FILE)
	if err != nil {
		return node, err
	}
	node.server, err = cluster.New(conf.Id, cluster_config)
	if err != nil {
		return node, err
	}

	node.log, err = log.Open(conf.LogFile)
	if err != nil {
		return node, err
	}

	node.config = conf

	node.commitCh = make(chan CommitInfo)

	//TODO: initialize other channels..
	node.mutex = &sync.RWMutex{}

	node.db = leveldb.Open(DB_DIR+string(conf.Id), nil)
	term, err := node.db.Get([]byte("term"), nil)
	if err != nil {
		node.sm.Term = 0
	} else {
		node.sm.Term = strconv.Atoi(string(term))
	}

	votedFor, err := node.db.Get([]byte("votedFor"), nil)
	if err != nil {
		node.sm.VotedFor = -1
	} else {
		node.sm.VotedFor = strconv.Atoi(string(votedFor))
	}

	return node
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
	for i := 0; i < len(conf.cluster); i++ {
		conf.Id = i
		conf.LogFile = LOG_DIR + string(i)
		rafts = append(rafts, NewRaftNode(conf))

		//FIXME:
		rafts[i].server, _ = cluster.New(i, conf)
	}
	return rafts
}
