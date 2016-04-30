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
	"sync"
)

type Client struct {
	conn   *net.TCPConn
	reader *bufio.Reader // a bufio Reader wrapper over conn
}

var fileServer []*exec.Cmd

func TestInit(t *testing.T) {
	StartAllServerProcess()
}

func StartServer(i int, restartflag string) {
	fileServer[i] = exec.Command("./assignment4", strconv.Itoa(i), restartflag)
	fileServer[i].Stdout = os.Stdout
	fileServer[i].Stdin = os.Stdin
	fileServer[i].Start()
}

func StartAllServerProcess() {
	conf := GetConfig(0)
	fileServer = make([]*exec.Cmd, len(conf.Peers))
	for i := 0; i < len(conf.Peers); i++ {
		os.RemoveAll("db/" + strconv.Itoa(i))
		os.RemoveAll("logs/" + strconv.Itoa(i))
		os.RemoveAll("fs/" + strconv.Itoa(i))
		StartServer(i, "false")
	}
	time.Sleep(30 * time.Second)
}

func KillServer(i int) {
	if err := fileServer[i - 1].Process.Kill(); err != nil {
		log.Fatal("failed to kill server: ", err)
	}
}

func KillAllServerProcess() {
	conf := GetConfig(0)
	for i := 1; i <= len(conf.Peers); i++ {
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

func (cl *Client) cas(filename string, version int, contents string, exptime int) (*utils.Cmd, error) {
	var cmd string
	if exptime == 0 {
		cmd = fmt.Sprintf("cas %s %d %d\r\n", filename, version, len(contents))
	} else {
		cmd = fmt.Sprintf("cas %s %d %d %d\r\n", filename, version, len(contents), exptime)
	}
	cmd += contents + "\r\n"
	return cl.sendRcv(cmd)
}

func (cl *Client) delete(filename string) (*utils.Cmd, error) {
	cmd := "delete " + filename + "\r\n"
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
	var client *Client
	raddr, err := net.ResolveTCPAddr("tcp", addr) // addr: "localhost:8080"
	if err == nil {
		conn, err := net.DialTCP("tcp", nil, raddr)
		if err == nil {
			client = &Client{conn: conn, reader: bufio.NewReader(conn)}
		} else {
			fmt.Println(err)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestBasicMain(t *testing.T) {
	var leader string

	// make client connection to all the servers
	nclients := 5
	clients := make([]*Client, nclients)
	addr := [5]string{"localhost:9001", "localhost:9002", "localhost:9003", "localhost:9004", "localhost:9005"}
	for i := 0; i < nclients; i++ {
		cl := mkClient(t, addr[i % 5])
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

	fmt.Println("Write success...")

	// Sleep for some time and let the entries be replicated at followers


	//make a client connection to follower and try to read
	leaderId := leader[len(leader) - 1:]
	l, _ := strconv.Atoi(leaderId)
	var foll int
	if l == 5 {
		foll = 1
	} else {
		foll = l + 1
	}
	time.Sleep(6 * time.Second)

	cl := mkClient(t, "localhost:900" + strconv.Itoa(foll))
	for i := 0; i < nclients; i++ {
		filename := fmt.Sprintf("DSsystem%v", i)
		m, err := cl.read(filename)
		expect(t, m, &utils.Cmd{Type: "C", Content: []byte("Distributed System is a zoo")}, "TestBasic: Read fille", err)
	}

	//	KillAllServerProcess()
	fmt.Println("TestBasic : Pass")
}

func TestRPC_BasicSequential(t *testing.T) {

	cl := mkClient(t, "localhost:9001")

	// Read non-existent file cs733net
	m, err := cl.read("cs733net")
	expect(t, m, &utils.Cmd{Type: "F"}, "file not found", err)


	// Read non-existent file cs733net
	m, err = cl.delete("cs733net")
	for err == nil && m.Type == "R" {
		cl = Redirect(t, m, cl)
		m, err = cl.delete("cs733net")
	}
	expect(t, m, &utils.Cmd{Type: "F"}, "file not found", err)

	// Write file cs733net
	data := "Cloud fun"
	m, err = cl.write("cs733net", data, 0)
	for err == nil && m.Type == "R" {
		cl = Redirect(t, m, cl)
		m, err = cl.write("cs733net", data, 0)
	}
	expect(t, m, &utils.Cmd{Type: "O"}, "write success", err)

	// Expect to read it back
	m, err = cl.read("cs733net")
	expect(t, m, &utils.Cmd{Type: "C", Content: []byte(data)}, "read my write", err)

	// CAS in new value
	version1 := m.Version
	data2 := "Cloud fun 2"
	// Cas new value
	m, err = cl.cas("cs733net", int(version1), data2, 0)
	for err == nil && m.Type == "R" {
		cl = Redirect(t, m, cl)
		m, err = cl.cas("cs733net", int(version1), data2, 0)
	}
	expect(t, m, &utils.Cmd{Type: "O"}, "cas success", err)

	// Expect to read it back
	m, err = cl.read("cs733net")
	expect(t, m, &utils.Cmd{Type: "C", Content: []byte(data2)}, "read my cas", err)

	// Expect Cas to fail with old version
	m, err = cl.cas("cs733net", int(version1), data, 0)
	for err == nil && m.Type == "R" {
		cl = Redirect(t, m, cl)
		m, err = cl.cas("cs733net", int(version1), data, 0)
	}
	expect(t, m, &utils.Cmd{Type: "V"}, "cas version mismatch", err)

	// Expect a failed cas to not have succeeded. Read should return data2.
	m, err = cl.read("cs733net")
	expect(t, m, &utils.Cmd{Type: "C", Content: []byte(data2)}, "failed cas to not have succeeded", err)

	// delete
	m, err = cl.delete("cs733net")
	for err == nil && m.Type == "R" {
		cl = Redirect(t, m, cl)
		m, err = cl.delete("cs733net")
	}
	expect(t, m, &utils.Cmd{Type: "O"}, "delete success", err)

	// Expect to not find the file
	m, err = cl.read("cs733net")
	expect(t, m, &utils.Cmd{Type: "F"}, "file not found", err)

	cl.close()

	//	KillAllServerProcess()

	fmt.Println("TestRPC_BasicSequential : Pass")
}

func TestRPC_Binary(t *testing.T) {
	//	StartAllServerProcess()
	cl := mkClient(t, "localhost:9003")
	defer cl.close()
	// Write binary contents
	data := "\x00\x01\r\n\x03" // some non-ascii, some crlf chars
	m, err := cl.write("binfile", data, 0)

	for err == nil && m.Type == "R" {
		cl = Redirect(t, m, cl)
		m, err = cl.write("binfile", data, 0)
	}
	expect(t, m, &utils.Cmd{Type: "O"}, "write success", err)

	// Expect to read it back
	m, err = cl.read("binfile")
	expect(t, m, &utils.Cmd{Type: "C", Content: []byte(data)}, "read my write", err)

	//	KillAllServerProcess()
	fmt.Println("TestRPC_Binary : Pass")
}

func TestRPC_BasicTimer(t *testing.T) {
	//	StartAllServerProcess()
	cl := mkClient(t, "localhost:9003")
	defer cl.close()

	// Write file cs733, with expiry time of 2 seconds
	str := "Cloud fun"
	m, err := cl.write("cs733", str, 2)
	for err == nil && m.Type == "R" {
		cl = Redirect(t, m, cl)
		m, err = cl.write("cs733", str, 2)
	}
	expect(t, m, &utils.Cmd{Type: "O"}, "write success", err)

	// Expect to read it back immediately.
	m, err = cl.read("cs733")
	expect(t, m, &utils.Cmd{Type: "C", Content: []byte(str)}, "read my cas", err)

	time.Sleep(3 * time.Second)
	// Expect to not find the file after expiry
	m, err = cl.read("cs733")
	expect(t, m, &utils.Cmd{Type: "F"}, "file not found", err)

	// Recreate the file with expiry time of 1 second
	m, err = cl.write("cs733", str, 1)
	for err == nil && m.Type == "R" {
		cl = Redirect(t, m, cl)
		m, err = cl.write("cs733", str, 1)
	}
	expect(t, m, &utils.Cmd{Type: "O"}, "file recreated", err)

	// Overwrite the file with expiry time of 4. This should be the new time.
	m, err = cl.write("cs733", str, 4)
	for err == nil && m.Type == "R" {
		cl = Redirect(t, m, cl)
		m, err = cl.write("cs733", str, 4)
	}
	expect(t, m, &utils.Cmd{Type: "O"}, "file overwriten with exptime=4", err)

	// The last expiry time was 3 seconds. We should expect the file to still be around 2 seconds later
	time.Sleep(2 * time.Second)
	// Expect the file to not have expired.
	m, err = cl.read("cs733")
	expect(t, m, &utils.Cmd{Type: "C", Content: []byte(str)}, "file to not expire until 4 sec", err)

	time.Sleep(3 * time.Second)
	// 5 seconds since the last write. Expect the file to have expired
	m, err = cl.read("cs733")
	expect(t, m, &utils.Cmd{Type: "F"}, "file not found after 4 sec", err)

	// Create the file with an expiry time of 10 sec. We"re going to delete it
	// then immediately create it. The new file better not get deleted.
	m, err = cl.write("cs733", str, 10)
	for err == nil && m.Type == "R" {
		cl = Redirect(t, m, cl)
		m, err = cl.write("cs733", str, 10)
	}
	expect(t, m, &utils.Cmd{Type: "O"}, "file created for delete", err)

	m, err = cl.delete("cs733")
	for err == nil && m.Type == "R" {
		cl = Redirect(t, m, cl)
		m, err = cl.delete("cs733")
	}
	expect(t, m, &utils.Cmd{Type: "O"}, "deleted ok", err)

	m, err = cl.write("cs733", str, 0) // No expiry
	for err == nil && m.Type == "R" {
		cl = Redirect(t, m, cl)
		m, err = cl.write("cs733", str, 0)
	}
	expect(t, m, &utils.Cmd{Type: "O"}, "file recreated", err)

	time.Sleep(1100 * time.Millisecond) // A little more than 1 sec
	m, err = cl.read("cs733")
	expect(t, m, &utils.Cmd{Type: "C"}, "file should not be deleted", err)

	//	KillAllServerProcess()
	fmt.Println("TestRPC_BasicTimer : Pass")
}

func TestRPC_ConcurrentWrites(t *testing.T) {
	//	StartAllServerProcess()
	nclients := 10 // It works for 500 clients and 5 iterations but takes time
	niters := 5
	clients := make([]*Client, nclients)
	addr := [5]string{"localhost:9001", "localhost:9002", "localhost:9003", "localhost:9004", "localhost:9005"}
	for i := 0; i < nclients; i++ {
		cl := mkClient(t, addr[i % 5])
		if cl == nil {
			t.Fatalf("Unable to create client #%d", i)
		}
		defer cl.close()
		clients[i] = cl
	}
	errCh := make(chan error, nclients)
	var sem sync.WaitGroup // Used as a semaphore to coordinate goroutines to begin concurrently
	sem.Add(1)
	ch := make(chan *utils.Cmd, nclients * niters) // channel for all replies
	for i := 0; i < nclients; i++ {
		go func(i int, cl *Client) {
			sem.Wait()
			for j := 0; j < niters; j++ {
				str := fmt.Sprintf("cl %d %d", i, j)
				m, err := cl.write("concWrite", str, 0)
				for err == nil && m.Type == "R" {
					cl = Redirect(t, m, cl)
					clients[i] = cl
					m, err = cl.write("concWrite", str, 0)
				}
				//			fmt.Printf("write cl %v %v successful \n",i,j)
				if err != nil {
					errCh <- err
					break
				} else {
					ch <- m
				}
			}
		}(i, clients[i])
	}
	time.Sleep(100 * time.Millisecond) // give goroutines a chance
	sem.Done()                         // Go!

	// There should be no errors
	for i := 0; i < nclients * niters; i++ {
		select {
		case m := <-ch:
			if m.Type != "O" {
				t.Fatalf("Concurrent write failed with kind=%c", m.Type)
			}
		case err := <-errCh:
			t.Fatal(err)
		}
	}
	m, _ := clients[0].read("concWrite")

	// Ensure the contents are of the form "cl <i> 4"
	// The last write of any client ends with " 4"
	if !(m.Type == "C" && strings.HasSuffix(string(m.Content), "4")) {
		t.Fatalf("Expected to be able to read after 1000 writes. Got msg = %v", m)
	}

	//	KillAllServerProcess()
	fmt.Println("TestRPC_ConcurrentWrites : Pass")
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
		errstr += fmt.Sprintf(" Got kind='%s', expected '%s'", response.Type, expected.Type)
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
