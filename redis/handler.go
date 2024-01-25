package redis

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/gofish2020/easyredis/abstract"
	"github.com/gofish2020/easyredis/cluster"
	"github.com/gofish2020/easyredis/engine"
	"github.com/gofish2020/easyredis/redis/connection"
	"github.com/gofish2020/easyredis/redis/parser"
	"github.com/gofish2020/easyredis/redis/protocol"
	"github.com/gofish2020/easyredis/tool/conf"
	"github.com/gofish2020/easyredis/tool/logger"
)

type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}

type RedisHandler struct {
	activeConn sync.Map

	//engine *engine.Engine

	engine abstract.Engine
}

func NewRedisHandler() *RedisHandler {

	var abEngine abstract.Engine
	if len(conf.GlobalConfig.Peers) > 0 {
		// 分布式
		logger.Debug("启动集群版")
		abEngine = cluster.NewCluster()
	} else {
		// 单机版
		logger.Debug("启动单机版")
		abEngine = engine.NewEngine()
	}
	return &RedisHandler{
		engine: abEngine,
	}
}

// 该方法是不同的conn复用的方法，要做的事情就是从conn中读取出符合RESP格式的数据；
// 然后针对消息格式，进行不同的业务处理
func (h *RedisHandler) Handle(ctx context.Context, conn net.Conn) {

	// 因为需要记录和conn相关的各种信息呢，所以定义 KeepConnection对象，将conn保存
	keepConn := connection.NewKeepConnection(conn)
	h.activeConn.Store(keepConn, struct{}{})

	outChan := parser.ParseStream(conn)
	for payload := range outChan {
		if payload.Err != nil {
			// 网络conn关闭
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF || strings.Contains(payload.Err.Error(), "use of closed network connection") {
				h.activeConn.Delete(keepConn)
				logger.Warn("client closed:" + keepConn.RemoteAddr())
				keepConn.Close()
				return
			}

			// 解析出错 protocol error
			errReply := protocol.NewGenericErrReply(payload.Err.Error())
			_, err := keepConn.Write(errReply.ToBytes())
			if err != nil {
				h.activeConn.Delete(keepConn)
				logger.Warn("client closed:" + keepConn.RemoteAddr() + " err info: " + err.Error())
				keepConn.Close()
				return
			}
			continue
		}

		if payload.Reply == nil {
			logger.Error("empty payload")
			continue
		}

		reply, ok := payload.Reply.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}

		logger.Debugf("%q", string(reply.ToBytes()))
		// 解析出redis命令，丢给存储引擎处理
		result := h.engine.Exec(keepConn, reply.RedisCommand)
		if result != nil {
			keepConn.Write(result.ToBytes())
		} else {
			keepConn.Write(protocol.NewUnknownErrReply().ToBytes())
		}
	}
}

func (h *RedisHandler) Close() error {

	logger.Info("handler shutting down...")

	h.activeConn.Range(func(key, value any) bool {
		keepConn := key.(*connection.KeepConnection)
		keepConn.Close()
		h.activeConn.Delete(key)
		return true
	})
	h.engine.Close()
	return nil
}
