package redis

import (
	"context"
	"net"
	"sync"

	"github.com/gofish2020/easyredis/tool/logger"
)

type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}

type RedisHandler struct {
	activeConn sync.Map
}

func NewRedisHandler() *RedisHandler {
	return &RedisHandler{}
}

func (h *RedisHandler) Handle(ctx context.Context, conn net.Conn) {

}

func (h *RedisHandler) Close() error {

	logger.Info("handler shutting down...")
	return nil
}
