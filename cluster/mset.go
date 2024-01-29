package cluster

import (
	"github.com/gofish2020/easyredis/abstract"
	"github.com/gofish2020/easyredis/redis/protocol"
)

// mset key value [key value...]
func mset(cluster *Cluster, c abstract.Connection, redisCommand [][]byte) protocol.Reply {

	// 基础校验
	if len(redisCommand) < 3 {
		return protocol.NewArgNumErrReply("mset")
	}

	argsNum := len(redisCommand) - 1
	if argsNum%2 != 0 {
		return protocol.NewArgNumErrReply("mset")
	}

	//1.从命令中，提取出 key value
	size := argsNum / 2
	keys := make([]string, 0, size)
	values := make(map[string]string)
	for i := 0; i < size; i++ {
		keys = append(keys, string(redisCommand[2*i+1]))
		values[keys[i]] = string(redisCommand[2*i+2])
	}

	//2.计算key映射的ip地址;  ip -> []string
	ipMap := cluster.groupByKeys(keys)

	// 3.说明keys映射为同一个ip地址，直接转发执行（不需要走分布式事务）
	if len(ipMap) == 1 {
		for ip := range ipMap {
			return cluster.Relay(ip, c, pushCmd(redisCommand, "Direct"))
		}
	}

	// 4.prepare阶段
	var respReply protocol.Reply = protocol.NewOkReply()
	// 事务id
	txId := cluster.newTxId()
	rollback := false
	for ip, keys := range ipMap {
		// txid mset key value [key value...]
		argsGroup := [][]byte{[]byte(txId), []byte("mset")}
		for _, key := range keys {
			argsGroup = append(argsGroup, []byte(key), []byte(values[key]))
		}
		//发送命令： prepare txid mset key value [key value...]
		reply := cluster.Relay(ip, c, pushCmd(argsGroup, "Prepare"))
		if protocol.IsErrReply(reply) { // 说明失败
			respReply = reply
			rollback = true
			break
		}
	}

	if rollback { // 如果prepare阶段失败， 向所有节点请求回滚
		rollbackTransaction(cluster, c, txId, ipMap)
	} else { // 所有节点都可以提交
		_, reply := commitTransaction(cluster, c, txId, ipMap)
		if reply != nil {
			respReply = reply
		}
	}
	return respReply
}
