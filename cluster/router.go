package cluster

import (
	"github.com/gofish2020/easyredis/abstract"
	"github.com/gofish2020/easyredis/redis/protocol"
)

type clusterFunc func(cluster *Cluster, conn abstract.Connection, args [][]byte) protocol.Reply

var clusterRouter = make(map[string]clusterFunc)

func init() {

	clusterRouter["set"] = defultFunc
	clusterRouter["get"] = defultFunc
}

func defultFunc(cluster *Cluster, conn abstract.Connection, redisCommand [][]byte) protocol.Reply {
	key := string(redisCommand[1])
	peer := cluster.consistHash.Get(key)
	return cluster.Relay(peer, conn, redisCommand) // 将命令转发

}
