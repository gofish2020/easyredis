package cluster

import (
	"sync"
	"time"

	"github.com/gofish2020/easyredis/abstract"
	"github.com/gofish2020/easyredis/engine"
	"github.com/gofish2020/easyredis/redis/protocol"
)

/*
事务对象
*/

const (
	maxPrepareTime    = 3 * time.Second
	waitBeforeCleanTx = 2 * maxPrepareTime
)

type transactionStatus int8

const (
	createdStatus    transactionStatus = 0
	preparedStatus   transactionStatus = 1
	committedStatus  transactionStatus = 2
	rolledBackStatus transactionStatus = 3
)

type Transaction struct {
	txId         string              // transaction id
	redisCommand [][]byte            // redis命令
	cluster      *Cluster            // 集群对象
	conn         abstract.Connection // socket连接
	dbIndex      int                 // 数据库索引

	writeKeys  []string  // 写key
	readKeys   []string  // 读key
	keysLocked bool      // 是否对写key/读key已经上锁
	undoLog    []CmdLine // 回滚日志

	status transactionStatus // 事务状态
	mu     *sync.Mutex       // 事务锁（操作事务对象的时候上锁）
}

func NewTransaction(txId string, cmdLine [][]byte, cluster *Cluster, c abstract.Connection) *Transaction {
	return &Transaction{
		txId:         txId,
		redisCommand: cmdLine,
		cluster:      cluster,
		conn:         c,
		dbIndex:      c.GetDBIndex(),
		status:       createdStatus,
		mu:           &sync.Mutex{},
	}
}

func (tx *Transaction) prepare() error {

	// 1.上锁
	tx.mu.Lock()
	defer tx.mu.Unlock()
	// 2.获取读写key
	readKeys, writeKeys := engine.GetRelatedKeys(tx.redisCommand)
	tx.readKeys = readKeys
	tx.writeKeys = writeKeys
	// 3. 锁定节点资源
	tx.locks()
	// 4.生成回滚日志
	tx.undoLog = tx.cluster.engine.GetUndoLogs(tx.dbIndex, tx.redisCommand)
	tx.status = preparedStatus
	return nil
}

func (tx *Transaction) rollback() error {
	if tx.status == rolledBackStatus { // no need to rollback a rolled-back transaction
		return nil
	}
	tx.locks()
	for _, cmdLine := range tx.undoLog { // 执行回滚日志
		tx.cluster.engine.ExecWithLock(tx.dbIndex, cmdLine)
	}
	tx.unlocks()
	tx.status = rolledBackStatus
	return nil
}

func (tx *Transaction) commit() protocol.Reply {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.status == committedStatus {
		return protocol.NewIntegerReply(0)
	}

	tx.locks()
	reply := tx.cluster.engine.ExecWithLock(tx.dbIndex, tx.redisCommand)
	if protocol.IsErrReply(reply) {
		tx.rollback() // commit 失败，自动回滚
		return reply
	}
	tx.status = committedStatus
	tx.unlocks()

	// 保留事务对象6s
	tx.cluster.delay.Add(waitBeforeCleanTx, "", func() {
		tx.cluster.transactionLock.Lock()
		delete(tx.cluster.transactions, tx.txId)
		tx.cluster.transactionLock.Unlock()
	})
	return reply
}

func (tx *Transaction) locks() {
	if !tx.keysLocked {
		tx.cluster.engine.RWLocks(tx.dbIndex, tx.readKeys, tx.writeKeys)
		tx.keysLocked = true
	}
}

func (tx *Transaction) unlocks() {
	if tx.keysLocked {
		tx.cluster.engine.RWUnLocks(tx.dbIndex, tx.readKeys, tx.writeKeys)
		tx.keysLocked = false
	}
}
