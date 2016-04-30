# Distributed File System

This system implements a fault-tolerant replicated system of `n` (= 5) servers, achieving consensus and self-stabilization using the Raft consensus algorithm. The file system API supports the basic commands: `read`, `write`, `cas` (compare and swap), and `delete`.

Note: Reads are not replicated - 

## Installation notes
### Requirements:
1. Golang 1.6+
2. ZeroMQ (libzmq 4.0.4)

### Building and testing
```
go get github.com/pratyakshs/cs733/assignment4/...
go build github.com/pratyakshs/cs733/assignment4
go test -v github.com/pratyakshs/cs733/assignment4 
```
### Usage
```
./assignment4 <server-ID>
telnet localhost <port-no>
```
`<server-ID>` can rage from 0 to 4.
`<port-no>` is currently set to `900<server-ID>`.

The configuration is (currently) hard coded, and is described in the `raft_config.json`. Future work is to eliminate the hard-coded configuration, and instead read it from a JSON file.

## Sample Usage
```
> ./assignment4 0 & 
> ./assignment4 1 &
> ./assignment4 2 &
> ./assignment4 3 &
> ./assignment4 4 &

> telnet localhost 9001
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
read foo
ERR_FILE_NOT_FOUND
write foo 3
abc
OK 1
read foo
CONTENTS 1 3 0
abc
cas foo 1 7 0 
abcdefg
OK 2
read foo
CONTENTS 2 7 0
abcdefg
q
ERR_CMD_ERR
Connection closed by foreign host.
```
## Command Specification

The format for each of the four commands is shown below,  

| Command  | Success Response | Error Response
|----------|-----|----------|
|read _filename_ \r\n| CONTENTS _version_ _numbytes_ _exptime remaining_\r\n</br>_content bytes_\r\n </br>| ERR_FILE_NOT_FOUND
|write _filename_ _numbytes_ [_exptime_]\r\n</br>_content bytes_\r\n| OK _version_\r\n| |
|cas _filename_ _version_ _numbytes_ [_exptime_]\r\n</br>_content bytes_\r\n| OK _version_\r\n | ERR\_VERSION _newversion_
|delete _filename_ \r\n| OK\r\n | ERR_FILE_NOT_FOUND

In addition the to the semantic error responses in the table above, all commands can get two additional errors. `ERR_CMD_ERR` is returned on a malformed command, `ERR_INTERNAL` on, well, internal errors.

For `write` and `cas` and in the response to the `read` command, the content bytes is on a separate line. The length is given by _numbytes_ in the first line.

Files can have an optional expiry time, _exptime_, expressed in seconds. A subsequent `cas` or `write` cancels an earlier expiry time, and imposes the new time. By default, _exptime_ is 0, which represents no expiry. 


## Contact
Pratyaksh Sharma.
pratyaksh _at_ me.com

-----
Tests are inspired by [Prerna Gupta's work](https://github.com/prernaguptaiitb/cs733/blob/master/assign4/mainserver_test.go).
