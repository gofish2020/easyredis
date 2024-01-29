package cluster

import (
	"github.com/gofish2020/easyredis/abstract"
	"github.com/gofish2020/easyredis/redis/protocol"
)

// 回滚事务
func rollbackTransaction(cluster *Cluster, c abstract.Connection, txId string, ipMap map[string][]string) {
	argsGroup := [][]byte{[]byte(txId)}
	// 向所有的ip发送回滚请求
	for ip := range ipMap {
		cluster.Relay(ip, c, pushCmd(argsGroup, "Rollback")) // Rollback txid
	}
}

// 提交事务
func commitTransaction(cluster *Cluster, c abstract.Connection, txId string, ipMap map[string][]string) ([]protocol.Reply, protocol.Reply) {

	result := make([]protocol.Reply, len(ipMap))
	var errReply protocol.Reply = nil
	argsGroup := [][]byte{[]byte(txId)}
	// 向所有的ip发送提交请求
	for ip := range ipMap {
		reply := cluster.Relay(ip, c, pushCmd(argsGroup, "Commit"))
		if protocol.IsErrReply(reply) { // 说明提交的时候失败了
			errReply = reply
			break
		}
		// 保存提交结果
		result = append(result, reply)
	}

	if errReply != nil {
		rollbackTransaction(cluster, c, txId, ipMap)
		result = nil
	}
	return result, errReply
}

func genTxKey(txId string) string {
	return "tx:" + txId
}

// ***********************Prepare/Commit/Rollback命令处理函数***********************
// prepare txid mset key value [key value...]
func prepareFunc(cluster *Cluster, conn abstract.Connection, redisCommand [][]byte) protocol.Reply {

	if len(redisCommand) < 3 {
		return protocol.NewArgNumErrReply("prepare")
	}

	txId := string(redisCommand[1])

	// 创建事务对象
	tx := NewTransaction(txId, redisCommand[2:], cluster, conn)

	// 存储对象
	cluster.transactionLock.Lock()
	cluster.transactions[txId] = tx
	cluster.transactionLock.Unlock()

	// prepare事务
	err := tx.prepare()
	if err != nil {
		return protocol.NewGenericErrReply(err.Error())
	}

	// 3s后如果事务还没有提交，自动回滚（避免长时间锁定）
	cluster.delay.Add(maxPrepareTime, genTxKey(txId), func() {
		tx.mu.Lock()
		defer tx.mu.Unlock()
		if tx.status == preparedStatus {
			tx.rollback()
			cluster.transactionLock.Lock()
			defer cluster.transactionLock.Unlock()
			delete(cluster.transactions, tx.txId)
		}
	})
	return protocol.NewOkReply()
}


// rollback txid
func rollbackFunc(cluster *Cluster, conn abstract.Connection, redisCommand [][]byte) protocol.Reply {

	if len(redisCommand) != 2 {
		return protocol.NewArgNumErrReply("rollback")
	}
	cluster.transactionLock.RLock()
	tx, ok := cluster.transactions[string(redisCommand[1])]
	cluster.transactionLock.RUnlock()
	if !ok {
		return protocol.NewIntegerReply(0) // 事务不存在
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	// 回滚事务
	err := tx.rollback()
	if err != nil {
		return protocol.NewGenericErrReply(err.Error())
	}

	// 延迟6s删除事务对象
	cluster.delay.Add(waitBeforeCleanTx, "", func() {
		cluster.transactionLock.Lock()
		defer cluster.transactionLock.Unlock()
		delete(cluster.transactions, tx.txId)
	})

	return protocol.NewIntegerReply(1)
}

// commit txid
func commitFunc(cluster *Cluster, conn abstract.Connection, redisCommand [][]byte) protocol.Reply {

	if len(redisCommand) != 2 {
		return protocol.NewArgNumErrReply("commit")
	}

	cluster.transactionLock.RLock()
	tx, ok := cluster.transactions[string(redisCommand[1])]
	cluster.transactionLock.RUnlock()
	if !ok {
		return protocol.NewIntegerReply(0) // 事务不存在
	}

	return tx.commit()
}
