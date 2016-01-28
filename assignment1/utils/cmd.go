package utils

import (
	"bufio"
	"errors"
	"strconv"
	"strings"
)

var MAX_FIRST_LINE_SIZE = 500
var MAX_CONTENT_SIZE = 1 << 32

type Cmd struct {
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
