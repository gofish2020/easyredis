package cluster

import (
	"github.com/gofish2020/easyredis/abstract"
	"github.com/gofish2020/easyredis/engine"
	"github.com/gofish2020/easyredis/redis/protocol"
)

/*
Redis集群
*/
type Cluster struct {
	clientFactory *RedisConnPool
	// Redis存储引擎
	engine *engine.Engine
}

func NewCluster() *Cluster {
	cluster := Cluster{
		clientFactory: NewRedisConnPool(),
		engine:        engine.NewEngine(),
	}
	return &cluster
}

func (cluster *Cluster) Exec(c abstract.Connection, redisCommand [][]byte) (result protocol.Reply) {

	return cluster.engine.Exec(c, redisCommand)
}
