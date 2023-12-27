package protocal

import "github.com/gofish2020/easyredis/utils"

// 简单错误 -Error message\r\n
type SimpleErrReply struct {
	Status string
}

func NewSimpleErrReply(status string) *SimpleErrReply {
	return &SimpleErrReply{
		Status: status,
	}
}

func (r *SimpleErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + utils.CRLF)
}

// 一般错误  -ERR unknown command 'asdf'
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

// 未知错误

type UnknownErrReply struct{}

func (r *UnknownErrReply) ToBytes() []byte {
	return []byte("-ERR unknown\r\n")
}

func NewUnknownErrReply() *UnknownErrReply {
	return &UnknownErrReply{}
}
