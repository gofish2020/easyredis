package engine

import (
	"strings"

	"github.com/gofish2020/easyredis/datastruct/dict"
	"github.com/gofish2020/easyredis/engine/payload"
	"github.com/gofish2020/easyredis/redis/connection"
	"github.com/gofish2020/easyredis/redis/protocal"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
)

type DB struct {

	// 数据库编号
	index int

	// 数据字典（协程安全）
	dataDict *dict.ConcurrentDict
	// 过期字典（协程安全）
	ttlDict *dict.ConcurrentDict
}

// 构造db对象
func newDB() *DB {
	db := &DB{
		dataDict: dict.NewConcurrentDict(dataDictSize),
		ttlDict:  dict.NewConcurrentDict(ttlDictSize),
	}
	return db
}

func (db *DB) SetIndex(index int) {
	db.index = index
}

func (db *DB) Exec(c *connection.KeepConnection, redisCommand [][]byte) protocal.Reply {

	return db.execNormalCommand(c, redisCommand)
}

func (db *DB) execNormalCommand(c *connection.KeepConnection, redisCommand [][]byte) protocal.Reply {

	cmdName := strings.ToLower(string(redisCommand[0]))

	// 从命令注册中心，获取命令的执行函数
	command, ok := commandCenter[cmdName]
	if !ok {
		return protocal.NewGenericErrReply("unknown command '" + cmdName + "'")
	}
	fun := command.execFunc
	return fun(db, redisCommand[1:])
}

/************** Data Access ***************/
// 获取内存中的数据
func (db *DB) GetEntity(key string) (*payload.DataEntity, bool) {

	// key 不存在
	val, exist := db.dataDict.Get(key)
	if !exist {
		return nil, false
	}

	dataEntity, ok := val.(*payload.DataEntity)
	if !ok {
		return nil, false
	}
	return dataEntity, true
}

// 保存数据到内存中 (插入 or 更新)
func (db *DB) PutEntity(key string, entity *payload.DataEntity) int {
	return db.dataDict.Put(key, entity)
}

// 保存数据到内存中 (插入 )
func (db *DB) PutIfAbsent(key string, entity *payload.DataEntity) int {
	return db.dataDict.PutIfAbsent(key, entity)
}

// 保存数据到内存中 (更新)
func (db *DB) PutIfExist(key string, entity *payload.DataEntity) int {
	return db.dataDict.PutIfExist(key, entity)
}
