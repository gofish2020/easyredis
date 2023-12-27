package engine

import (
	"fmt"
	"net"
	"runtime/debug"
	"strings"

	"github.com/gofish2020/easyredis/redis/protocal"
	"github.com/gofish2020/easyredis/tool/logger"
)

type Engine struct {
}

func NewEngine() *Engine {
	return &Engine{}
}

func (e *Engine) Exec(c net.Conn, redisCommand [][]byte) protocal.Reply {

	var result protocal.Reply

	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = protocal.NewUnknownErrReply()
		}
	}()

	commandName := strings.ToLower(string(redisCommand[0]))

	switch commandName {
	case "info":
	case "command":
	}

	return result
}
