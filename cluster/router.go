package cluster

import (
	"strings"

	"github.com/gofish2020/easyredis/abstract"
	"github.com/gofish2020/easyredis/redis/protocol"
)

type clusterFunc func(cluster *Cluster, conn abstract.Connection, args [][]byte) protocol.Reply

var clusterRouter = make(map[string]clusterFunc)

func registerClusterRouter(cmd string, f clusterFunc) {
	cmd = strings.ToLower(cmd)
	clusterRouter[cmd] = f
}
func init() {

	// 在集群节点上注册的命令
	registerClusterRouter("Set", defultFunc)
	registerClusterRouter("Get", defultFunc)
	registerClusterRouter("MSet", mset)

	registerClusterRouter("Prepare", prepareFunc)
	registerClusterRouter("Rollback", rollbackFunc)
	registerClusterRouter("Commit", commitFunc)

	// 表示命令直接在存储引擎上执行命令
	registerClusterRouter("Direct", directFunc)
}

func defultFunc(cluster *Cluster, conn abstract.Connection, redisCommand [][]byte) protocol.Reply {
	key := string(redisCommand[1])
	// 计算key所属的节点
	peer := cluster.consistHash.Get(key)
	return cluster.Relay(peer, conn, pushCmd(redisCommand, "Direct")) // 将命令转发至节点，直接执行（不用再重复计算key所属节点）
}

// 直接在存储引擎上执行命令
func directFunc(cluster *Cluster, conn abstract.Connection, redisCommand [][]byte) protocol.Reply {
	return cluster.engine.Exec(conn, popCmd(redisCommand))
}
