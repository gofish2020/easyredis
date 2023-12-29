package protocal

import (
	"bytes"
	"strconv"

	"github.com/gofish2020/easyredis/utils"
)

// 二进制安全 多个bulk *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
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

// 二进制安全 单个bulk $3\r\nkey\r\n
type BulkReply struct {
	Arg []byte
}

func NewBulkReply(arg []byte) *BulkReply {

	return &BulkReply{
		Arg: arg,
	}
}
func (b *BulkReply) ToBytes() []byte {
	if b.Arg == nil {
		return NewNullBulkReply().ToBytes()
	}
	return []byte("$" + strconv.Itoa(len(b.Arg)) + utils.CRLF + string(b.Arg) + utils.CRLF)
}

// null bulk   $-1\r\n
var nullBulkReply = &NullBulkReply{}

type NullBulkReply struct{}

func (n *NullBulkReply) ToBytes() []byte {
	return []byte("$-1" + utils.CRLF)
}

func NewNullBulkReply() *NullBulkReply {
	return nullBulkReply
}
