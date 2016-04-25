package utils

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var MAX_FIRST_LINE_SIZE = 500
var MAX_CONTENT_SIZE = 1 << 32

type Cmd struct {
	ClientID int64
	Type     string
	Filename string
	Numbytes int
	Version  int64
	Exptime  int64
	Content  []byte
}

func ReadCmd(reader *bufio.Reader) (*Cmd, error) {
	buf := make([]byte, MAX_FIRST_LINE_SIZE)
	cmdLen := ReadTillEOL(reader, buf)
	if cmdLen <= 0 {
		return &Cmd{}, errors.New("line corrupted")
	}

	cmd, err := ParseCmd(reader, buf, cmdLen)
	if err != nil {
		return cmd, err
	} else if cmdLen >= len(buf) {
		return cmd, errors.New("line too long")
	}
	return cmd, nil
}

func ParseCmd(reader *bufio.Reader, buf []byte, cmdLen int) (*Cmd, error) {
	cmdstr := string(buf[:cmdLen])
	fields := strings.Fields(cmdstr)
	l := len(fields)
	var err error
	var exptime int

	cmd := Cmd{Type: fields[0], Filename: fields[1]}

	switch fields[0] {
	case "write":
		if l > 4 || l < 3 {
			return &cmd, errors.New("invalid number of arguments")
		}
		cmd.Numbytes, err = strconv.Atoi(fields[2])
		if err != nil {
			return &cmd, errors.New("cannot parse numbytes")
		}
		if l == 4 {
			exptime, err = strconv.Atoi(fields[3])
			if err != nil {
				return &cmd, errors.New("cannot parse exptime")
			}
			cmd.Exptime = int64(exptime)
		}
		contentBuf := make([]byte, cmd.Numbytes)
		cnterr := ReadNBytes(reader, contentBuf, cmd.Numbytes)
		if !cnterr {
			return &cmd, errors.New("content not of expected length")
		}
		cmd.Content = contentBuf

	case "read", "delete":
		if l != 2 {
			return &cmd, errors.New("invalid number of arguments")
		}

	case "cas":
		if l > 5 || l < 4 {
			return &cmd, errors.New("invalid number of arguments")
		}
		cmd.Version, err = strconv.ParseInt(fields[2], 10, 8)
		if err != nil {
			return &cmd, errors.New("cannot parse version")
		}
		cmd.Numbytes, err = strconv.Atoi(fields[3])
		if err != nil {
			return &cmd, errors.New("cannot parse numbytes")
		}
		if l == 5 {
			cmd.Exptime, err = strconv.ParseInt(fields[4], 10, 8)
			if err != nil {
				return &cmd, errors.New("cannot parse exptime")
			}
		}
		contentBuf := make([]byte, cmd.Numbytes)
		cnterr := ReadNBytes(reader, contentBuf, cmd.Numbytes)
		if !cnterr {
			return &cmd, errors.New("content not of expected length")
		}
		cmd.Content = contentBuf
	}
	return &cmd, nil
}

func ReadNBytes(reader *bufio.Reader, buf []byte, n int) bool {
	for i := 0; i < n; i++ {
		ch, err := reader.ReadByte()
		if err != nil {
			return false
		}
		buf[i] = ch
	}
	ch1, _ := reader.ReadByte()
	ch2, _ := reader.ReadByte()
	return ch1 == '\r' && ch2 == '\n'
}

func ReadTillEOL(reader *bufio.Reader, buf []byte) int {
	i := 0
	for {
		ch, err := reader.ReadByte()
		if err != nil {
			return -1
		}
		if i >= len(buf) {
			return i
		}

		if ch == '\r' {
			ch, err := reader.ReadByte()
			if err != nil {
				return -1
			}
			if ch == '\n' {
				return i
			} else {
				buf[i] = '\r'
				buf[i+1] = ch
				i += 1
			}
		} else {
			buf[i] = ch
		}
		i += 1
	}
	return i
}

func GetMsg(clientid int64, reader *bufio.Reader) (msg *Cmd, msgerr error, fatalerr error) {
	buf := make([]byte, MAX_FIRST_LINE_SIZE)
	msg, msgerr, fatalerr = parseFirst(clientid, reader, buf)
	if fatalerr == nil {
		if msg.Type == "w" || msg.Type == "c" || msg.Type == "C" /*CONTENTS*/ {
			msg.Content, fatalerr = parseSecond(reader, msg.Numbytes)
		}
	}
	return msg, msgerr, fatalerr
}

// The first line is all text. Some errors (such as unknown command, non-integer numbytes
// etc. are fatal errors; it is not possible to know where the command ends, and so we cannot
// recover to read the next command. The other errors are still errors, but we can at least
// consume the entirety of the current command.
func parseFirst(clientid int64, reader *bufio.Reader, buf []byte) (msg *Cmd, msgerr error, fatalerr error) {
	var err error
	var msgstr string
	var fields []string

	if msgstr, err = fillLine(buf, reader); err != nil {
		// read until EOL
		return nil, nil, err
	}
	// validation functions checkN and toInt help avoid repeated use of "if err != nil ". Once
	// "err" is set, these functions don't do anything.
	checkN := func(fields []string, min int) {
		if err != nil {
			return
		}
		if len(fields) < min {
			err = errors.New("Incorrect number of fields in command")
			fatalerr = err
		}
	}
	// convert fields[fieldnum] to int if no error
	toInt := func(fieldnum int, recoverable bool) int {
		var i int
		if fatalerr != nil {
			return 0
		}
		i, err = strconv.Atoi(fields[fieldnum])
		if err != nil {
			if recoverable {
				msgerr = err
			} else {
				fatalerr = err
			}
		}
		return i
	}
	version := 0
	numbytes := 0
	exptime := 0
	response := false
	var kind string

	fields = strings.Fields(msgstr)
	switch fields[0] {
	case "read":
		checkN(fields, 2)
	case "write": // write <filename> <numbytes> [<exptime>]
		checkN(fields, 3)
		numbytes = toInt(2, false)
		if len(fields) >= 4 {
			exptime = toInt(3, true)
		}
	case "cas": // cas <filename> <version> <numbytes> [<exptime>]
		checkN(fields, 4)
		version = toInt(2, true)
		numbytes = toInt(3, false)
		if len(fields) == 5 {
			exptime = toInt(4, true)
		}
	case "delete":
		checkN(fields, 1)

	case "CONTENTS":
		checkN(fields, 4)
		version = toInt(1, true)
		numbytes = toInt(2, false)
		exptime = toInt(3, true)
		response = true

	case "OK":
		checkN(fields, 1)
		if len(fields) > 1 {
			version = toInt(1, true)
		}
		response = true
	case "ERR_VERSION":
		checkN(fields, 2)
		version = toInt(1, true)
		kind = "V"
		response = true
	case "ERR_FILE_NOT_FOUND":
		kind = "F"
		response = true
	case "ERR_CMD_ERR":
		kind = "M" // 'C' is taken for contents
		response = true
	case "ERR_INTERNAL":
		kind = "I"
		response = true
	default:
		fatalerr = fmt.Errorf("Command %s not recognized", fields[0])
	}
	if fatalerr == nil {
		var filename = ""
		if kind == "" {
			kind = string(fields[0][0]) // first char
		}
		if !response {
			filename = fields[1]
		}
		return &Cmd{Type: kind, ClientID: clientid, Filename: filename, Numbytes: numbytes, Exptime: int64(exptime), Version: int64(version)}, msgerr, nil
	} else {
		return nil, nil, fatalerr
	}
}

func parseSecond(reader *bufio.Reader, numbytes int) (buf []byte, err error) {
	if numbytes > MAX_CONTENT_SIZE {
		return nil, errors.New(fmt.Sprintf("numbytes cannot exceed %d", MAX_CONTENT_SIZE))
	}
	buf = make([]byte, numbytes+2) // includes CRLF
	_, err = io.ReadFull(reader, buf)
	if err == nil {
		last := len(buf) - 1
		if !(buf[last-1] == '\r' && buf[last] == '\n') {
			err = errors.New("Expected CRLF at end of contents line")
		}

	}
	if err == nil {
		return buf[:numbytes], nil // without the crlf
	} else {
		return nil, err
	}
}

func fillLine(buf []byte, reader *bufio.Reader) (string, error) {
	var err error
	count := 0
	for {
		var c byte
		if c, err = reader.ReadByte(); err != nil {
			return "", err
		}
		if c == '\r' {
			if c, err = reader.ReadByte(); err != nil || c != '\n' {
				return "", io.EOF
			}
			return string(buf[:count]), nil
		}
		if count >= len(buf) {
			return "", errors.New("Line too long")
		}
		buf[count] = c
		count += 1
	}
}
