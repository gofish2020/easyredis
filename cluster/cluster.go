package cluster

import (
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/gofish2020/easyredis/abstract"
	"github.com/gofish2020/easyredis/engine"
	"github.com/gofish2020/easyredis/engine/payload"
	"github.com/gofish2020/easyredis/redis/protocol"
	"github.com/gofish2020/easyredis/tool/conf"
	"github.com/gofish2020/easyredis/tool/consistenthash"
	"github.com/gofish2020/easyredis/tool/idgenerator"
	"github.com/gofish2020/easyredis/tool/logger"
	"github.com/gofish2020/easyredis/tool/timewheel"
)

/*
Redis集群
*/

type CmdLine = [][]byte

const (
	replicas = 100 // 副本数量
)

type Cluster struct {
	// 当前的ip地址
	self string
	// socket连接池
	clientFactory Factory
	// Redis存储引擎
	engine *engine.Engine

	// 一致性hash
	consistHash *consistenthash.Map

	// 雪花算法，生成唯一guid
	snowflake *idgenerator.IDGenerator

	// 分布式事务
	transactionLock sync.RWMutex
	transactions    map[string]*Transaction

	delay *timewheel.Delay
}

func NewCluster() *Cluster {
	cluster := Cluster{
		clientFactory: NewRedisConnPool(),
		engine:        engine.NewEngine(),
		consistHash:   consistenthash.New(replicas, nil),
		self:          conf.GlobalConfig.Self,
		snowflake:     idgenerator.MakeGenerator(conf.GlobalConfig.Self),
		delay:         timewheel.NewDelay(),
		transactions:  make(map[string]*Transaction),
	}

	// 一致性hash初始化
	contains := make(map[string]struct{})
	peers := make([]string, 0, len(conf.GlobalConfig.Peers)+1)
	// 去重
	for _, peer := range conf.GlobalConfig.Peers {
		if _, ok := contains[peer]; ok {
			continue
		}
		peers = append(peers, peer)
	}

	if _, ok := contains[cluster.self]; !ok {
		peers = append(peers, cluster.self)
	}
	// 添加到集群
	cluster.consistHash.Add(peers...)
	return &cluster
}

func (cluster *Cluster) Exec(c abstract.Connection, redisCommand [][]byte) (result protocol.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = protocol.NewUnknownErrReply()
		}
	}()

	name := strings.ToLower(string(redisCommand[0]))
	routerFunc, ok := clusterRouter[name]
	if !ok {
		return protocol.NewGenericErrReply("unknown command '" + name + "' or not support command in cluster mode")
	}
	return routerFunc(cluster, c, redisCommand)
}

func (cluster *Cluster) Close() {
	cluster.engine.Close()
}

func (cluster *Cluster) ForEach(dbIndex int, cb func(key string, data *payload.DataEntity, expiration *time.Time) bool) {
	cluster.engine.ForEach(dbIndex, cb)
}
