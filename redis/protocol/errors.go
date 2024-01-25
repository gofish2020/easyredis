package protocol

import "github.com/gofish2020/easyredis/utils"

// 自定义错误 - xxxxx
type SimpleErrReply struct {
	Status string
}

func NewSimpleErrReply(status string) *SimpleErrReply {
	return &SimpleErrReply{
		Status: status,
	}
}

func (s *SimpleErrReply) ToBytes() []byte {
	return []byte("-" + s.Status)
}

// 一般错误  -ERR xxxxx
type GenericErrReply struct {
	Status string
}

func NewGenericErrReply(status string) *GenericErrReply {
	return &GenericErrReply{
		Status: status,
	}
}

func (s *GenericErrReply) ToBytes() []byte {
	return []byte("-ERR " + s.Status + utils.CRLF)
}

// 未知错误 -ERR unknown
type UnknownErrReply struct{}

func (r *UnknownErrReply) ToBytes() []byte {
	return []byte("-ERR unknown\r\n")
}

func NewUnknownErrReply() *UnknownErrReply {
	return &UnknownErrReply{}
}

// 命令参数数量错误
type ArgNumErrReply struct {
	Cmd string
}

func (r *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR wrong number of arguments for '" + r.Cmd + "' command\r\n")
}

func NewArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{
		Cmd: cmd,
	}
}

// 底层数据类型错误
type WrongTypeErrReply struct{}

var wrongTypeErrBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

func (r *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}

func NewWrongTypeErrReply() *WrongTypeErrReply {
	return &WrongTypeErrReply{}
}

// 语法错误
type SyntaxErrReply struct{}

var syntaxErrBytes = []byte("-Err syntax error\r\n")
var syntaxErrReply = &SyntaxErrReply{}

func (s *SyntaxErrReply) ToBytes() []byte {
	return syntaxErrBytes
}

func NewSyntaxErrReply() *SyntaxErrReply {
	return syntaxErrReply
}

// 是否为Err

func IsErrReply(reply Reply) bool {
	return reply.ToBytes()[0] == '-'
}
