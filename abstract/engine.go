package abstract

import (
	"time"

	"github.com/gofish2020/easyredis/engine/payload"
	"github.com/gofish2020/easyredis/redis/protocol"
)

type Engine interface {
	Exec(c Connection, redisCommand [][]byte) (result protocol.Reply)
	ForEach(dbIndex int, cb func(key string, data *payload.DataEntity, expiration *time.Time) bool)
	Close()
}
