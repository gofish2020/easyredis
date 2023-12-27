package protocal

import (
	"bytes"
	"strconv"

	"github.com/gofish2020/easyredis/utils"
)

type Reply interface {
	ToBytes() []byte
}

// 二进制安全的多数值返回 *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
type MultiBulkReply struct {
	RedisCommand [][]byte
}

func NewMultiBulkReply(command [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		RedisCommand: command,
	}
}

func (r *MultiBulkReply) ToBytes() []byte {
	num := len(r.RedisCommand)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(num) + utils.CRLF)
	for _, command := range r.RedisCommand {
		if command == nil {
			buf.WriteString("$-1" + utils.CRLF)
		} else {
			length := len(command)
			buf.WriteString("$" + strconv.Itoa(length) + utils.CRLF + string(command) + utils.CRLF)
		}
	}
	return buf.Bytes()
}

