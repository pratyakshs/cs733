package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"bytes"
	"encoding/gob"
	"errors"
	"os"
	"strings"

	"bufio"

	"github.com/pratyakshs/cs733/assignment4/utils"
	"github.com/syndtr/goleveldb/leveldb"
)

var ERR_VERSION = "ERR_VERSION "
var ERR_FILE_NOT_FOUND = "ERR_FILE_NOT_FOUND"
var ERR_CMD_ERR = "ERR_CMD_ERR"
var ERR_INTERNAL = "ERR_INTERNAL"

var MAX_CLIENTS int64 = 100000000000

type Metadata struct {
	Version  int64
	Exptime  time.Time
	Numbytes int
	Timer    *time.Timer
}

type FS struct {
	sync.RWMutex
	Dir map[string]*Metadata
	DB  *leveldb.DB
}

//var fs FS

//var dirMutex = &sync.RWMutex{}
//var dir = make(map[string]*Metadata, 1000)
//var gversion int64 = 0

func sendResponse(con net.Conn, cmd *utils.Cmd) bool {
	var err error
	write := func(data []byte) {
		if err != nil {
			return
		}
		_, err = con.Write(data)
	}
	var resp string
	switch cmd.Type {
	case "C": // read response
		resp = fmt.Sprintf("CONTENTS %d %d %d", cmd.Version, cmd.Numbytes, cmd.Exptime)
	case "O":
		resp = "OK "
		if cmd.Version > 0 {
			resp += strconv.FormatInt(cmd.Version, 10)
		}
	case "F":
		resp = ERR_FILE_NOT_FOUND
	case "V":
		resp = ERR_VERSION + strconv.FormatInt(cmd.Version, 10)
	case "M":
		resp = ERR_CMD_ERR
	case "I":
		resp = ERR_INTERNAL
	default:
		fmt.Printf("Unknown response kind '%c'", cmd.Type)
		return false
	}
	resp += "\r\n"
	write([]byte(resp))
	if cmd.Type == "C" {
		write(cmd.Content)
		write([]byte{'\r', '\n'})
	}
	return err == nil
}

func doWrite(fs *FS, gversion *int64, msg *utils.Cmd) *utils.Cmd {
	fs.Lock()
	defer fs.Unlock()
	return internalWrite(fs, gversion, msg)
}

func (fi *Metadata) cancelTimer() {
	if fi.Timer != nil {
		fi.Timer.Stop()
		fi.Timer = nil
	}
}

func internalWrite(fs *FS, gversion *int64, msg *utils.Cmd) *utils.Cmd {
	fi := fs.Dir[msg.Filename]
	if fi != nil {
		fi.cancelTimer()
	} else {
		fi = &Metadata{}
	}

	*gversion += 1
	err := fs.DB.Put([]byte(msg.Filename), msg.Content, nil)

	if err != nil {
		log.Fatal(err)
	}

	fi.Version = int64(*gversion)

	var absexptime time.Time
	if msg.Exptime > 0 {
		dur := time.Duration(msg.Exptime) * time.Second
		absexptime = time.Now().Add(dur)
		timerFunc := func(name string, ver int64) func() {
			return func() {
				doDelete(fs, &utils.Cmd{Type: "D",
					Filename: name,
					Version:  ver})
			}
		}(msg.Filename, int64(*gversion))

		fi.Timer = time.AfterFunc(dur, timerFunc)
	}
	fi.Exptime = absexptime
	fs.Dir[msg.Filename] = fi

	return ok(*gversion)
}

func ok(version int64) *utils.Cmd {
	return &utils.Cmd{Type: "O", Version: version}
}

func doRead(fs *FS, cmd *utils.Cmd) *utils.Cmd {
	fs.RLock()
	defer fs.RUnlock()
	fi := fs.Dir[cmd.Filename]

	if fi != nil {
		var remainingTime int64
		remainingTime = 0
		if fi.Timer != nil {
			remainingTime := int(fi.Exptime.Sub(time.Now()))
			if remainingTime < 0 {
				remainingTime = 0
			}
		}
		data, err := fs.DB.Get([]byte(cmd.Filename), nil)
		if err != nil {
			log.Fatal(err)
		}
		return &utils.Cmd{
			Type:     "C",
			Filename: cmd.Filename,
			Content:  data,
			Numbytes: fi.Numbytes,
			Exptime:  remainingTime,
			Version:  fi.Version,
		}
	} else {
		return &utils.Cmd{Type: "F"} // file not found
	}
}

func doCas(fs *FS, gversion *int64, msg *utils.Cmd) *utils.Cmd {
	fs.Lock()
	defer fs.Unlock()

	if fi := fs.Dir[msg.Filename]; fi != nil {
		if msg.Version != fi.Version {
			return &utils.Cmd{Type: "V", Version: fi.Version}
		}
	}
	return internalWrite(fs, gversion, msg)
}

func doDelete(fs *FS, msg *utils.Cmd) *utils.Cmd {
	fs.Lock()
	defer fs.Unlock()
	fi := fs.Dir[msg.Filename]
	if fi != nil {
		if msg.Version > 0 && fi.Version != msg.Version {
			// non-zero msg.Version indicates a delete due to an expired timer
			return nil // nothing to do
		}
		fi.cancelTimer()
		delete(fs.Dir, msg.Filename)
		//TODO: remove from leveldb
		return ok(0)
	} else {
		return &utils.Cmd{Type: "F"} // file not found
	}

}

//func handleClient(con net.Conn, db *leveldb.DB) {
//	reader := bufio.NewReader(con)
//	for {
//		cmd, err := utils.ReadCmd(reader)
//		if err != nil {
//			break
//		}
//
//		switch cmd.Type {
//		case "write":
//			sendResponse(con, doWrite(cmd, db))
//		case "read":
//			sendResponse(con, doRead(cmd, db))
//		case "cas":
//			sendResponse(con, doCas(cmd, db))
//		case "delete":
//			sendResponse(con, doDelete(cmd, db))
//		}
//	}
//}

func ProcessMsg(fs *FS, gversion *int64, msg *utils.Cmd) *utils.Cmd {
	switch msg.Type {
	case "read":
		return doRead(fs, msg)
	case "write":
		return doWrite(fs, gversion, msg)
	case "cas":
		return doCas(fs, gversion, msg)
	case "delete":
		return doDelete(fs, msg)
	}

	// Default: Internal error. Shouldn't come here since
	// the msg should have been validated earlier.
	return &utils.Cmd{Type: "I"}
}

//func serverinit() {
//	//config, err := raft.GetConfig()
//
//	db, err := leveldb.OpenFile("pratyakshs.db", nil)
//	defer db.Close()
//	if err != nil {
//		log.Fatal(err)
//		return
//	}
//
//	listen, err := net.Listen("tcp", PORT)
//	if err != nil {
//		log.Fatal(err)
//		return
//	}
//
//	for {
//		connect, err := listen.Accept()
//		if err != nil {
//			log.Fatal(err)
//			continue // Exit here?
//		}
//		go handleClient(connect, db)
//	}
//}

type FSConfig struct {
	Id      int
	Address string
	//	Port int
}

type ClusterConfig struct {
	Peers []PeerConfig
}

type ClientResponse struct {
	Message *utils.Cmd
	Err     error
}

type Server struct {
	sync.RWMutex
	fsconf        []FSConfig
	ClientChanMap map[int64]chan ClientResponse
	rn            RaftNode
	fileMap       *FS
	gversion      int64
}

func makeFSNetConfig(conf Config) []FSConfig {
	fsconf := make([]FSConfig, len(conf.Cluster))

	for j := 0; j < len(conf.Cluster); j++ {
		fsconf[j].Id = conf.Cluster[j].Id
		fsconf[j].Address = conf.Cluster[j].ClientAddress
	}
	return fsconf
}

func makeRaftNetConfig(conf ClusterConfig) []NetConfig {
	clusterconf := make([]NetConfig, len(conf.Peers))

	for j := 0; j < len(conf.Peers); j++ {
		clusterconf[j].Id = conf.Peers[j].Id
		clusterconf[j].Host = strings.Split(conf.Peers[j].Address, ":")[0]
		clusterconf[j].Port, _ = strconv.Atoi(strings.Split(conf.Peers[j].Address, ":")[1])
	}
	return clusterconf
}

type MsgEntry struct {
	Data utils.Cmd
}

func (server *Server) getAddress(id int) string {
	// find address of this server
	var address string
	for i := 0; i < len(server.fsconf); i++ {
		if id == server.fsconf[i].Id {
			address = server.fsconf[i].Address
			break
		}
	}
	return address
}

func check(obj interface{}) {
	if obj != nil {
		fmt.Println(obj)
		os.Exit(1)
	}
}

func serverMain(id int, restartFlag string) {
	var server Server
	gob.Register(MsgEntry{})
	var clientid int64 = int64(id)

	// make map for mapping client id with corresponding receiving clientcommitchannel
	server.ClientChanMap = make(map[int64]chan ClientResponse)

	conf_all := GetConfig(id)
	var conf ClusterConfig
	conf.Peers = conf_all.Cluster

	// fsconf stores array of the id and addresses of all file servers
	server.fsconf = makeFSNetConfig(conf_all)

	// make a map for storing file info
	server.fileMap = &FS{Dir: make(map[string]*Metadata, 10000)}

	server.gversion = 0
	// find address of this server
	address := server.getAddress(id)

	// start the file server
	tcpaddr, err := net.ResolveTCPAddr("tcp", address)
	check(err)
	tcp_acceptor, err := net.ListenTCP("tcp", tcpaddr)

	fmt.Println(id, "POOP")
	fmt.Println(id, "tcpaddr", tcpaddr)
	fmt.Println(id, "tcp_acceptor", tcp_acceptor)
	fmt.Println(id, "err", err)
	fmt.Println(id, "POOPA")
	check(err)

	// make raft server object
	//raftconf := makeRaftNetConfig(conf)

	conf_all.Id = id
	server.rn, err = NewRaftNode(conf_all)

	//if restartFlag == "true" {
	//	server.rn = RestartNode(id, raftconf)
	//} else {
	//	server.rn = BringNodeUp(id, raftconf)
	//}

	initRaft()

	// start listening on raft commit channel
	go server.ListenCommitChannel()

	// start raft server to process events
	go server.rn.eventLoop()

	// start accepting connection from clients
	for {
		tcp_conn, err := tcp_acceptor.AcceptTCP()
		check(err)

		// assign id and commit chan to client
		clientid = (clientid + int64(5)) % MAX_CLIENTS
		clientCommitCh := make(chan ClientResponse)
		server.Lock()
		server.ClientChanMap[clientid] = clientCommitCh
		server.Unlock()

		// go and serve the client connection
		go server.serve(clientid, clientCommitCh, tcp_conn)
	}

}

func (server *Server) serve(clientid int64, clientCommitCh chan ClientResponse, conn *net.TCPConn) {
	reader := bufio.NewReader(conn)
	var res ClientResponse
	var response *utils.Cmd
	for {

		msg, msgerr, fatalerr := utils.GetMsg(clientid, reader)

		//		msg.ClientId=clientid
		if fatalerr != nil {
			sendResponse(conn, &utils.Cmd{Type: "M"})
			conn.Close()
			break
		}

		if msgerr != nil {
			if (!sendResponse(conn, &utils.Cmd{Type: "M"})) {
				conn.Close()
				break
			}
			continue
		}

		// if message not of read type
		if msg.Type == "w" || msg.Type == "c" || msg.Type == "d" {
			dbytes, err := encode(*msg)

			if err != nil {
				if (!sendResponse(conn, &utils.Cmd{Type: "I"})) {
					conn.Close()
					break
				}
				continue
			}
			// append the msg to the raft node log
			server.rn.Append(dbytes)
			fmt.Println("heybro1")
			//wait for the msg to appear on the client commit channel
			res = <-clientCommitCh
			fmt.Println("heybro2")
			if res.Err != nil {
				msgContent := server.getAddress(server.rn.LeaderId())
				//				fmt.Printf("Leader address : %v\n", rc.LeaderId())
				sendResponse(conn, &utils.Cmd{Type: "R", Content: []byte(msgContent)})
				conn.Close()
				break
			}
			response = res.Message
			//		fmt.Printf("Response Message %v\n", string(response.Contents))

		} else if msg.Type == "read" {
			response = ProcessMsg(server.fileMap, &(server.gversion), msg)
		}

		if !sendResponse(conn, response) {
			conn.Close()
			break
		}
	}
}

func assert(val bool) {
	if !val {
		fmt.Println("Assertion Failed")
		os.Exit(1)
	}

}

func encode(data utils.Cmd) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	me := MsgEntry{Data: data}
	err := enc.Encode(me)
	return buf.Bytes(), err
}

func decode(dbytes []byte) (utils.Cmd, error) {
	buf := bytes.NewBuffer(dbytes)
	enc := gob.NewDecoder(buf)
	var me MsgEntry
	err := enc.Decode(&me)
	return me.Data, err
}

func (server *Server) ListenCommitChannel() {
	time.Sleep(100 * time.Second)
	getMsg := func(index int64) {
		emsg, err := server.rn.Get(index)
		if err != nil {
			fmt.Printf("ListenCommitChannel: Error in getting message 4")
			assert(err == nil)
		}

		dmsg, err := decode(emsg)
		if err != nil {
			fmt.Printf("ListenCommitChannel: Error in decoding message 3")
			assert(err == nil)
		}
		response := ProcessMsg(server.fileMap, &(server.gversion), &dmsg)
		server.Lock()
		if ch, ok := server.ClientChanMap[dmsg.ClientID]; ok {
			ch <- ClientResponse{response, nil}
		}
		server.Unlock()
	}

	var prevLogIndexProcessed = int64(-1)
	for {
		//listen on commit channel of raft node
		commitval := <-server.rn.CommitChannel()
		if commitval.Err != nil {
			//Redirect the client. Assume the server for which this server voted is the leader. So, redirect it there.
			dmsg, err := decode(commitval.Data)
			if err != nil {
				fmt.Printf("ListenCommitChannel: Error in decoding message 1")
				assert(err == nil)
			}
			server.Lock()
			if ch, ok := server.ClientChanMap[dmsg.ClientID]; ok {
				ch <- ClientResponse{nil, errors.New("ERR_REDIRECT")}
			}
			server.Unlock()

		} else {
			//check if there are missing or duplicate commits
			if commitval.Index <= prevLogIndexProcessed {
				// already processed. So continue
				continue
			}
			// if missing get them
			for i := prevLogIndexProcessed + 1; i < commitval.Index; i++ {
				getMsg(i)
			}

			dmsg, err := decode(commitval.Data)
			if err != nil {
				fmt.Printf("ListenCommitChannel: Error in decoding message 3")
				assert(err == nil)
			}
			// process the message and send response to client
			response := ProcessMsg(server.fileMap, &(server.gversion), &dmsg)
			//			fmt.Printf("Response: %v", *response)
			//server.ClientChanMap[dmsg.ClientId]<-ClientEntry{dmsg,nil}
			server.Lock()
			if ch, ok := server.ClientChanMap[dmsg.ClientID]; ok {
				ch <- ClientResponse{response, nil}
			}
			server.Unlock()
			prevLogIndexProcessed = commitval.Index
		}

	}

}

func main() {
	id := os.Args[1]
	restartFlag := os.Args[2]
	serverId, _ := strconv.Atoi(id)
	serverMain(serverId, restartFlag)
}
