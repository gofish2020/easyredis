package redis

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/gofish2020/easyredis/engine"
	"github.com/gofish2020/easyredis/redis/parser"
	"github.com/gofish2020/easyredis/redis/protocal"
	"github.com/gofish2020/easyredis/tool/logger"
)

type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}

type RedisHandler struct {
	activeConn sync.Map

	engine *engine.Engine
}

func NewRedisHandler() *RedisHandler {
	return &RedisHandler{
		engine: engine.NewEngine(),
	}
}

// 该方法是不同的conn复用的方法，要做的事情就是从conn中读取出符合RESP格式的数据；
// 然后针对消息格式，进行不同的业务处理
func (h *RedisHandler) Handle(ctx context.Context, conn net.Conn) {

	h.activeConn.Store(conn, struct{}{})

	outChan := parser.ParseStream(conn)
	for payload := range outChan {
		if payload.Err != nil {
			// 网络conn关闭
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF || strings.Contains(payload.Err.Error(), "use of closed network connection") {
				h.activeConn.Delete(conn)
				conn.Close()
				logger.Warn("client closed:" + conn.RemoteAddr().String())
				return
			}

			// 解析出错 protocol error
			errReply := protocal.NewGenericErrReply(payload.Err.Error())
			_, err := conn.Write(errReply.ToBytes())
			if err != nil {
				h.activeConn.Delete(conn)
				conn.Close()
				logger.Warn("client closed:" + conn.RemoteAddr().String() + " err info: " + err.Error())
				return
			}
			continue
		}

		if payload.Reply == nil {
			logger.Error("empty payload")
			continue
		}

		reply, ok := payload.Reply.(*protocal.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}

		logger.Debugf("%q", string(reply.ToBytes()))

		result := h.engine.Exec(conn, reply.RedisCommand)
		if result != nil {
			conn.Write(result.ToBytes())
		} else {
			conn.Write(protocal.NewUnknownErrReply().ToBytes())
		}
	}
}

func (h *RedisHandler) Close() error {

	logger.Info("handler shutting down...")

	h.activeConn.Range(func(key, value any) bool {
		conn := key.(net.Conn)
		conn.Close()
		h.activeConn.Delete(key)
		return true
	})
	return nil
}
