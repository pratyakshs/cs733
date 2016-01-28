package main

import (
	"bufio"
	"fmt"
	"github.com/pratyakshs/cs733/assignment1/utils"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"net"
	"os"
)

const PORT = ":8080"

var ERR_VERSION = []byte("ERR_VERSION ")
var ERR_FILE_NOT_FOUND = []byte("ERR_FILE_NOT_FOUND\r\n")
var ERR_CMD_ERR = []byte("ERR_CMD_ERR\r\n")
var ERR_INTERNAL = []byte("ERR_INTERNAL\r\n")

var MAX_FIRST_LINE_SIZE = 500
var MAX_CONTENT_SIZE = 1 << 32

func doWrite(cmd *Cmd, con net.Conn) {
}

func doRead(cmd *Cmd, con net.Conn) {
}

func doCas(cmd *Cmd, con net.Conn) {
}

func doDelete(cmd *Cmd, con net.Conn) {
}

func handleClient(con net.Conn) {
	reader := bufio.NewReader(con)

	for {
		cmd, err := utils.ReadCmd(reader)
		if err != nil {
			break
		}

		switch cmd.Type {
		case "write":
			doWrite(cmd, con)
		case "read":
			doRead(cmd, con)
		case "cas":
			doCas(cmd, con)
		case "delete":
			doDelete(cmd, con)
		}
	}
}

func serverMain() {
	listen, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	for {
		connect, err := listen.Accept()
		if err != nil {
			log.Fatal(err)
			continue // Exit here?
		}
		go handleClient(connect)
	}
}

func leveldbTest() {
	db, _ := leveldb.OpenFile("/tmp/temo.db", nil)
	_ = db.Put([]byte("key"), []byte("value"), nil)

	data, _ := db.Get([]byte("key"), nil)
	fmt.Println(string(data))

	defer db.Close()

}

func main() {
	// leveldbTest()
	serverMain()
}
