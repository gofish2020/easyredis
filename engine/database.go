package engine

import (
	"strings"
	"time"

	"github.com/gofish2020/easyredis/abstract"
	"github.com/gofish2020/easyredis/datastruct/dict"
	"github.com/gofish2020/easyredis/engine/payload"
	"github.com/gofish2020/easyredis/redis/protocol"
	"github.com/gofish2020/easyredis/tool/logger"
	"github.com/gofish2020/easyredis/tool/timewheel"
)

type CmdLine = [][]byte

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

	writeAof func(redisCommand [][]byte)

	delay *timewheel.Delay
}

// 构造db对象
func newDB(delay *timewheel.Delay) *DB {
	db := &DB{
		dataDict: dict.NewConcurrentDict(dataDictSize),
		ttlDict:  dict.NewConcurrentDict(ttlDictSize),
		writeAof: func(redisCommand [][]byte) {},
		delay:    delay,
	}
	return db
}

func newBasicDB(delay *timewheel.Delay) *DB {
	db := &DB{
		dataDict: dict.NewConcurrentDict(dataDictSize),
		ttlDict:  dict.NewConcurrentDict(ttlDictSize),
		writeAof: func(redisCommand [][]byte) {},
		delay:    delay,
	}
	return db
}

func (db *DB) SetIndex(index int) {
	db.index = index
}

func (db *DB) Exec(c abstract.Connection, redisCommand [][]byte) protocol.Reply {

	return db.execNormalCommand(c, redisCommand)
}

func (db *DB) execNormalCommand(c abstract.Connection, redisCommand [][]byte) protocol.Reply {

	cmdName := strings.ToLower(string(redisCommand[0]))

	// 从命令注册中心，获取命令的执行函数
	command, ok := commandCenter[cmdName]
	if !ok {
		return protocol.NewGenericErrReply("unknown command '" + cmdName + "'")
	}
	fun := command.execFunc
	return fun(db, redisCommand[1:])
}

func genExpireTask(key string) string {
	return "expire:" + key
}

func (db *DB) addDelayAt(key string, expireTime time.Time) {
	if db.delay != nil {
		db.delay.AddAt(expireTime, genExpireTask(key), func() {
			logger.Debug("expire: " + key)
			db.IsExpire(key)
		})
	}
}

// 设定key过期
func (db *DB) ExpireAt(key string, expireTime time.Time) {

	// 在ttlDict中设置key的过期时间
	db.ttlDict.Put(key, expireTime)
	// 设置过期延迟任务
	db.addDelayAt(key, expireTime)
}

func (db *DB) cancelDelay(key string) {
	if db.delay != nil {
		db.delay.Cancel(genExpireTask(key))
	}
}

// 设定key不过期
func (db *DB) Persist(key string) {
	db.ttlDict.Delete(key)
	db.cancelDelay(key)
}

// 删除key(单个)
func (db *DB) Remove(key string) {
	db.ttlDict.Delete(key)
	db.dataDict.Delete(key)
	// 从时间轮中删除任务
	db.cancelDelay(key)
}

// 删除多个key 返回成功删除个数
func (db *DB) Removes(keys ...string) int64 {
	var deleted int64 = 0
	for _, key := range keys {
		_, exist := db.dataDict.Get(key)
		if exist {
			deleted++
			db.Remove(key)
		}
	}
	return deleted
}

// 相对时间（过期）s
func (db *DB) TTL(key string) int64 {
	val, result := db.ttlDict.Get(key)
	if !result {
		return -1
	}
	diff := time.Until(val.(time.Time)) / time.Second
	return int64(diff)
}

// 相对时间（过期）ms
func (db *DB) PTTL(key string) int64 {
	val, result := db.ttlDict.Get(key)
	if !result {
		return -1
	}
	diff := time.Until(val.(time.Time)) / time.Millisecond
	return int64(diff)
}

// 绝对时间（过期） s
func (db *DB) ExpireTime(key string) int64 {
	val, result := db.ttlDict.Get(key)
	if !result {
		return -1
	}
	return val.(time.Time).Unix()
}

// 绝对时间（过期）ms
func (db *DB) PExpireTime(key string) int64 {
	val, result := db.ttlDict.Get(key)
	if !result {
		return -1
	}
	return val.(time.Time).UnixMilli()
}

// 判断是否key没有过期时间
func (db *DB) IsPersist(key string) bool {
	_, result := db.ttlDict.Get(key)
	return !result
}

// 判断key是否已过期
func (db *DB) IsExpire(key string) bool {
	val, result := db.ttlDict.Get(key)
	if !result {
		return false
	}
	expireTime, _ := val.(time.Time)
	isExpire := time.Now().After(expireTime)
	if isExpire { // 如果过期，主动删除
		db.Remove(key)
	}
	return isExpire
}

/************** Data Access ***************/
// 获取内存中的数据
func (db *DB) GetEntity(key string) (*payload.DataEntity, bool) {

	// key 不存在
	val, exist := db.dataDict.Get(key)
	if !exist {
		return nil, false
	}
	// key是否过期（主动检测一次）
	if db.IsExpire(key) {
		return nil, false
	}
	// 返回内存数据
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
