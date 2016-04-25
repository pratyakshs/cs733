package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pratyakshs/cs733/assignment4/utils"
)

type Client struct {
	conn   *net.TCPConn
	reader *bufio.Reader // a bufio Reader wrapper over conn
}

var fileServer []*exec.Cmd

func StartServer(i int, restartflag string) {
	fileServer[i-1] = exec.Command("./assignment4", strconv.Itoa(i-1), restartflag)
	fileServer[i-1].Stdout = os.Stdout
	fileServer[i-1].Stdin = os.Stdin
	fileServer[i-1].Start()
}

func StartAllServerProcess() {
	conf := GetConfig(0)
	fileServer = make([]*exec.Cmd, len(conf.Cluster))
	for i := 1; i <= len(conf.Cluster); i++ {
		os.RemoveAll("myLogDir" + strconv.Itoa(i) + "/logfile")
		os.RemoveAll("myStateDir" + strconv.Itoa(i) + "/mystate")
		StartServer(i, "false")
	}
	time.Sleep(7 * time.Second)
}

func KillServer(i int) {
	if err := fileServer[i-1].Process.Kill(); err != nil {
		log.Fatal("failed to kill server: ", err)
	}
}

func KillAllServerProcess() {
	conf := GetConfig(0)
	for i := 1; i <= len(conf.Cluster); i++ {
		KillServer(i)
		os.RemoveAll("myLogDir" + strconv.Itoa(i) + "/logfile")
		os.RemoveAll("myStateDir" + strconv.Itoa(i) + "/mystate")
	}
	time.Sleep(2 * time.Second)
}

func Redirect(t *testing.T, cmd *utils.Cmd, cl *Client) (c *Client) {
	redirectAddress := string(cmd.Content)
	cl.close()
	cl = mkClient(t, redirectAddress)
	return cl
}

func (cl *Client) close() {
	if cl != nil && cl.conn != nil {
		cl.conn.Close()
		cl.conn = nil
	}
}

func (cl *Client) write(filename string, contents string, exptime int) (*utils.Cmd, error) {
	var cmd string
	if exptime == 0 {
		cmd = fmt.Sprintf("write %s %d\r\n", filename, len(contents))
	} else {
		cmd = fmt.Sprintf("write %s %d %d\r\n", filename, len(contents), exptime)
	}
	cmd += contents + "\r\n"
	return cl.sendRcv(cmd)
}

var errNoConn = errors.New("Connection is closed")

func (cl *Client) sendRcv(str string) (cmd *utils.Cmd, err error) {
	if cl.conn == nil {
		return nil, errNoConn
	}
	err = cl.send(str)
	if err == nil {
		cmd, err = cl.rcv()
	}
	return cmd, err
}

func (cl *Client) send(str string) error {
	if cl.conn == nil {
		return errNoConn
	}
	_, err := cl.conn.Write([]byte(str))
	if err != nil {
		err = fmt.Errorf("Write error in SendRaw: %v", err)
		cl.conn.Close()
		cl.conn = nil
	}
	return err
}

func (cl *Client) rcv() (cmd *utils.Cmd, err error) {
	// we will assume no errors in server side formatting
	line, err := cl.reader.ReadString('\n')
	if err == nil {
		cmd, err = parseFirst(line)
		if err != nil {
			return nil, err
		}
		if cmd.Type == "C" {
			contents := make([]byte, cmd.Numbytes)
			var c byte
			for i := 0; i < cmd.Numbytes; i++ {
				if c, err = cl.reader.ReadByte(); err != nil {
					break
				}
				contents[i] = c
			}
			if err == nil {
				cmd.Content = contents
				cl.reader.ReadByte() // \r
				cl.reader.ReadByte() // \n
			}
		}
	}
	if err != nil {
		cl.close()
	}
	return cmd, err
}

func parseFirst(line string) (cmd *utils.Cmd, err error) {
	//	fmt.Printf("%v",line)
	fields := strings.Fields(line)
	cmd = &utils.Cmd{}

	// Utility function fieldNum to int
	toInt := func(fieldNum int) int64 {
		var i int64
		if err == nil {
			if fieldNum >= len(fields) {
				err = errors.New(fmt.Sprintf("Not enough fields. Expected field #%d in %s\n", fieldNum, line))
				return 0
			}
			i, err = strconv.ParseInt(fields[fieldNum], 10, 64)
		}
		return i
	}

	if len(fields) == 0 {
		return nil, errors.New("Empty line. The previous command is likely at fault")
	}
	switch fields[0] {
	case "OK": // OK [version]
		cmd.Type = "O"
		if len(fields) > 1 {
			cmd.Version = toInt(1)
		}
	case "CONTENTS": // CONTENTS <version> <numbytes> <exptime> \r\n
		cmd.Type = "C"
		cmd.Version = toInt(1)
		cmd.Numbytes = int(toInt(2))
		cmd.Exptime = toInt(3)
	case "ERR_VERSION":
		cmd.Type = "V"
		cmd.Version = toInt(1)
	case "ERR_FILE_NOT_FOUND":
		cmd.Type = "F"
	case "ERR_CMD_ERR":
		cmd.Type = "M"
	case "ERR_INTERNAL":
		cmd.Type = "I"
	case "ERR_REDIRECT":
		cmd.Type = "R"
		if len(fields) < 2 {
			cmd.Content = []byte("localhost:9005")
		} else {
			cmd.Content = []byte(fields[1])
		}
	default:
		err = errors.New("Unknown response " + fields[0])
	}
	if err != nil {
		return nil, err
	} else {
		return cmd, nil
	}
}

func mkClient(t *testing.T, addr string) *Client {
	fmt.Println(addr)
	var client *Client
	raddr, err := net.ResolveTCPAddr("tcp", addr) // addr: "localhost:8080"
	if err == nil {
		conn, err := net.DialTCP("tcp", nil, raddr)
		if err == nil {
			client = &Client{conn: conn, reader: bufio.NewReader(conn)}
		} else {
			fmt.Println("nahi galat")
			fmt.Println(err)
		}
	}
	if err != nil {
		fmt.Printf("Error in mkclient\n")
		t.Fatal(err)
	}
	return client
}

func TestBasicMain(t *testing.T) {

	StartAllServerProcess()

	var leader string

	time.Sleep(100 * time.Second)

	// make client connection to all the servers
	nclients := 5
	clients := make([]*Client, nclients)
	addr := [5]string{"localhost:9001", "localhost:9002", "localhost:9003", "localhost:9004", "localhost:9005"}
	for i := 0; i < nclients; i++ {
		cl := mkClient(t, addr[i%5])
		if cl == nil {
			t.Fatalf("Unable to create client #%d", i)
		}
		defer cl.close()
		clients[i] = cl
	}

	fmt.Println("Clients connected", nclients)

	// Try a write command at each server. All the clients except the one connected to leader should be redirected
	for i := 0; i < nclients; i++ {
		str := "Distributed System is a zoo"
		filename := fmt.Sprintf("DSsystem%v", i)
		fmt.Println(filename)
		cl := clients[i]
		m, err := cl.write(filename, str, 0)
		for err == nil && m.Type == "R" {
			leader = string(m.Content)
			cl = Redirect(t, m, cl)
			clients[i] = cl
			m, err = cl.write(filename, str, 0)
		}
		expect(t, m, &utils.Cmd{Type: "O"}, "Test_Basic: Write success", err)
	}

	// Sleep for some time and let the entries be replicated at followers
	time.Sleep(3 * time.Second)

	//make a client connection to follower and try to read
	leaderId := leader[len(leader)-1:]
	l, _ := strconv.Atoi(leaderId)
	var foll int
	if l == 5 {
		foll = 1
	} else {
		foll = l + 1
	}

	cl := mkClient(t, "localhost:900"+strconv.Itoa(foll))
	for i := 0; i < nclients; i++ {
		filename := fmt.Sprintf("DSsystem%v", i)
		m, err := cl.read(filename)
		expect(t, m, &utils.Cmd{Type: "C", Content: []byte("Distributed System is a zoo")}, "TestBasic: Read fille", err)
	}

	//	KillAllServerProcess()
	fmt.Println("TestBasic : Pass")
}

func (cl *Client) read(filename string) (*utils.Cmd, error) {
	cmd := "read " + filename + "\r\n"
	return cl.sendRcv(cmd)
}

func expect(t *testing.T, response *utils.Cmd, expected *utils.Cmd, errstr string, err error) {
	if err != nil {
		KillAllServerProcess()
		t.Fatal("Unexpected error: " + err.Error())
	}
	ok := true
	if response.Type != expected.Type {
		ok = false
		errstr += fmt.Sprintf(" Got kind='%c', expected '%c'", response.Type, expected.Type)
	}
	if expected.Version > 0 && expected.Version != response.Version {
		ok = false
		errstr += " Version mismatch"
	}
	if response.Type == "C" {
		if expected.Content != nil &&
			bytes.Compare(response.Content, expected.Content) != 0 {
			ok = false
		}
	}
	if !ok {
		KillAllServerProcess()
		t.Fatal("Expected " + errstr)
	}
}
