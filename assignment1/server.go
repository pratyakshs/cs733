package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net"
)

const PORT = ":8080"

var ERR_VERSION = []byte("ERR_VERSION ")
var ERR_FILE_NOT_FOUND = []byte("ERR_FILE_NOT_FOUND\r\n")
var ERR_CMD_ERR = []byte("ERR_CMD_ERR\r\n")
var ERR_INTERNAL = []byte("ERR_INTERNAL\r\n")

func doWrite(cmdBytes [][]byte, con net.Conn) {
	fmt.Println("doWrite", cmdBytes)
}

func doRead(cmdBytes [][]byte, con net.Conn) {
	fmt.Println("doRead", cmdBytes)
}
func doCas(cmdBytes [][]byte, con net.Conn) {
	fmt.Println("doCas", cmdBytes)
}
func doDelete(cmdBytes [][]byte, con net.Conn) {
	fmt.Println("doDelete", cmdBytes)
}

func handleClient(con net.Conn) {
	reader := bufio.NewReader(con)
	for {
		bytesRec, _, err := reader.ReadLine()
		if err != nil {
			log.Println(err)
			con.Close()
			return
		}
		splitCmd := bytes.Fields(bytesRec)
		cmdLen := len(splitCmd)
		if cmdLen == 0 {
			con.Write(ERR_CMD_ERR)
			return
		}
		switch string(splitCmd[0]) {
		case "write":
			doWrite(splitCmd[1:], con)
		case "read":
			doRead(splitCmd[1:], con)
		case "cas":
			doCas(splitCmd[1:], con)
		case "delete":
			doDelete(splitCmd[1:], con)
		default:
			con.Write(ERR_CMD_ERR)
			return
		}
	}
}

func serverMain() {
	listen, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	for {
		connect, err := listen.Accept()
		if err != nil {
			log.Fatal(err)
			continue
		}
		go handleClient(connect)
	}
}

func main() {
	serverMain()
}
