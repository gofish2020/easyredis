package protocal

import "github.com/gofish2020/easyredis/utils"

// +OK\r\n
var okReply = &OKReply{}

type OKReply struct{}

func (r *OKReply) ToBytes() []byte {
	return []byte("+OK" + utils.CRLF)
}

func NewOkReply() *OKReply {
	return okReply
}

// +PONG\r\n
var pongReply = &PONGReply{}

type PONGReply struct{}

func (r *PONGReply) ToBytes() []byte {
	return []byte("+PONG" + utils.CRLF)
}

func NewPONGReply() *PONGReply {
	return pongReply
}
