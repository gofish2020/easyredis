package protocol

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/gofish2020/easyredis/utils"
)

// 空数组 empty array
var emptyMultiBulkReply = &EmptyMultiBulkReply{}

type EmptyMultiBulkReply struct {
}

func (e *EmptyMultiBulkReply) ToBytes() []byte {
	return []byte("*0\r\n")
}

func NewEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return emptyMultiBulkReply
}

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

// Integer   :3\r\n
type IntegerReply struct {
	Integer int64
}

func (i *IntegerReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(i.Integer, 10) + utils.CRLF)
}

func NewIntegerReply(integer int64) *IntegerReply {
	return &IntegerReply{Integer: integer}
}

// Mix
func NewMixReply() *MixReply {
	return &MixReply{}
}

type MixReply struct {
	replies []Reply
}

func (m *MixReply) ToBytes() []byte {
	num := len(m.replies)
	var str strings.Builder
	str.WriteString("*" + strconv.Itoa(num) + utils.CRLF) // example: *3\r\n
	for _, reply := range m.replies {
		str.Write(reply.ToBytes())
	}
	return []byte(str.String())
}

func (m *MixReply) Append(replies ...Reply) {
	m.replies = append(m.replies, replies...)
}
