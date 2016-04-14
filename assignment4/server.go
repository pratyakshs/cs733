package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/pratyakshs/cs733/assignment1/utils"
	"github.com/syndtr/goleveldb/leveldb"
)

const PORT = ":8080"

var ERR_VERSION = "ERR_VERSION "
var ERR_FILE_NOT_FOUND = "ERR_FILE_NOT_FOUND"
var ERR_CMD_ERR = "ERR_CMD_ERR"
var ERR_INTERNAL = "ERR_INTERNAL"

type Metadata struct {
	Version  int64
	Exptime  time.Time
	Numbytes int
	Timer    *time.Timer
}

var dirMutex = &sync.RWMutex{}
var dir = make(map[string]*Metadata, 1000)
var gversion int64 = 0

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

func doWrite(cmd *utils.Cmd, db *leveldb.DB) *utils.Cmd {

	dirMutex.Lock()
	defer dirMutex.Unlock()
	fi := dir[cmd.Filename]
	if fi != nil {
		if fi.Timer != nil {
			fi.Timer.Stop()
			fi.Timer = nil
		}
	} else {
		fi = &Metadata{}
	}

	if cmd.Exptime != 0 {
		dur := time.Duration(cmd.Exptime) * time.Second
		fi.Exptime = time.Now().Add(dur)
		// create timer for deletion
		timerFunc := func(name string, ver int64) func() {
			return func() {
				doDelete(&utils.Cmd{Type: "delete",
					Filename: name,
					Version:  ver}, db)
			}
		}(cmd.Filename, gversion)

		fi.Timer = time.AfterFunc(dur, timerFunc)

	}
	err := db.Put([]byte(cmd.Filename), cmd.Content, nil)

	if err != nil {
		log.Fatal(err)

	}

	gversion += 1
	fi.Version = gversion
	fi.Numbytes = cmd.Numbytes
	dir[cmd.Filename] = fi

	return &utils.Cmd{Type: "O", Version: gversion}
}

func doRead(cmd *utils.Cmd, db *leveldb.DB) *utils.Cmd {
	dirMutex.RLock()
	defer dirMutex.RUnlock()
	fi := dir[cmd.Filename]

	//if fi != nil {
	//	// exptime := 0
	//	if fi.Timer != nil {
	//		// exptime := fi.Exptime.Sub(time.Now())
	//		// if exptime < 0 {
	//		// 	exptime = 0
	//		// }
	//	}
	//} else {
	//
	//}
	//data, err := db.Get([]byte(cmd.Filename), nil)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//response := fmt.Sprintf("CONTENTS %d %d %d\r\n", fi.Version, fi.Numbytes, fi.Exptime)
	//con.Write([]byte(response))
	//con.Write(data)
	//con.Write([]byte("\r\n"))

	if fi != nil {
		var remainingTime int64
		remainingTime = 0
		if fi.Timer != nil {
			remainingTime := int(fi.Exptime.Sub(time.Now()))
			if remainingTime < 0 {
				remainingTime = 0
			}
		}
		data, err := db.Get([]byte(cmd.Filename), nil)
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

func doCas(cmd *utils.Cmd, db *leveldb.DB) *utils.Cmd {
	fi := dir[cmd.Filename]
	if fi != nil {
		if cmd.Version != fi.Version {
			return &utils.Cmd{Type: "V", Version: fi.Version}
		}
		if fi.Timer != nil {
			fi.Timer.Stop()
			fi.Timer = nil
		}
	} else {
		fi = &Metadata{}
	}

	if cmd.Exptime != 0 {
		dur := time.Duration(cmd.Exptime) * time.Second
		fi.Exptime = time.Now().Add(dur)
		// create timer for deletion
		timerFunc := func(name string, ver int64) func() {
			return func() {
				doDelete(&utils.Cmd{Type: "delete",
					Filename: name,
					Version:  ver}, db)
			}
		}(cmd.Filename, gversion)

		fi.Timer = time.AfterFunc(dur, timerFunc)

	}
	err := db.Put([]byte(cmd.Filename), cmd.Content, nil)

	if err != nil {
		log.Fatal(err)

	}

	gversion += 1
	fi.Version = gversion
	fi.Numbytes = cmd.Numbytes
	dir[cmd.Filename] = fi

	return &utils.Cmd{Type: "O", Version: gversion}
}

func doDelete(cmd *utils.Cmd, db *leveldb.DB) *utils.Cmd {
	dirMutex.Lock()
	defer dirMutex.Unlock()
	fi := dir[cmd.Filename]
	if fi != nil {
		if cmd.Version > 0 && fi.Version != cmd.Version {
			// non-zero msg.Version indicates a delete due to an expired timer
			return nil // nothing to do
		}

		if fi.Timer != nil {
			fi.Timer.Stop()
			fi.Timer = nil
		}
		delete(dir, cmd.Filename)
		return &utils.Cmd{Type: "O", Version: 0}
	} else {
		return &utils.Cmd{Type: "F"} // file not found
	}
}

func handleClient(con net.Conn, db *leveldb.DB) {
	reader := bufio.NewReader(con)
	for {
		cmd, err := utils.ReadCmd(reader)
		if err != nil {
			break
		}

		switch cmd.Type {
		case "write":
			sendResponse(con, doWrite(cmd, db))
		case "read":
			sendResponse(con, doRead(cmd, db))
		case "cas":
			sendResponse(con, doCas(cmd, db))
		case "delete":
			sendResponse(con, doDelete(cmd, db))
		}
	}
}

func main() {
	db, err := leveldb.OpenFile("pratyakshs.db", nil)
	defer db.Close()
	if err != nil {
		log.Fatal(err)
		return
	}

	listen, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatal(err)
		return
	}

	for {
		connect, err := listen.Accept()
		if err != nil {
			log.Fatal(err)
			continue // Exit here?
		}
		go handleClient(connect, db)
	}
}
