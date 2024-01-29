package engine

import (
	"fmt"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gofish2020/easyredis/abstract"
	"github.com/gofish2020/easyredis/aof"
	"github.com/gofish2020/easyredis/engine/payload"
	"github.com/gofish2020/easyredis/pubhub"
	"github.com/gofish2020/easyredis/redis/protocol"
	"github.com/gofish2020/easyredis/tool/conf"
	"github.com/gofish2020/easyredis/tool/logger"
	"github.com/gofish2020/easyredis/tool/timewheel"
)

// 存储引擎，负责数据的CRUD
type Engine struct {
	// *DB
	dbSet []*atomic.Value
	// 时间轮(延迟任务)
	delay *timewheel.Delay
	// Append Only File
	aof *aof.AOF

	// 订阅

	hub *pubhub.Pubhub
}

func NewEngine() *Engine {

	engine := &Engine{}

	engine.delay = timewheel.NewDelay()
	// 多个dbSet
	engine.dbSet = make([]*atomic.Value, conf.GlobalConfig.Databases)
	for i := 0; i < conf.GlobalConfig.Databases; i++ {
		// 创建 *db
		db := newDB(engine.delay)
		db.SetIndex(i)
		// 保存到 atomic.Value中
		dbset := &atomic.Value{}
		dbset.Store(db)
		// 赋值到 dbSet中
		engine.dbSet[i] = dbset
	}

	engine.hub = pubhub.NewPubsub()
	// 启用AOF日志
	if conf.GlobalConfig.AppendOnly {
		// 创建*AOF对象
		aof, err := aof.NewAOF(conf.GlobalConfig.Dir+"/"+conf.GlobalConfig.AppendFilename, engine, true, conf.GlobalConfig.AppendFsync)
		if err != nil {
			panic(err)
		}
		engine.aof = aof
		// 设定每个db，使用aof写入日志
		engine.aofBindEveryDB()
	}
	return engine
}

func (e *Engine) aofBindEveryDB() {
	for _, dbSet := range e.dbSet {
		db := dbSet.Load().(*DB)
		db.writeAof = func(redisCommand [][]byte) {
			if conf.GlobalConfig.AppendOnly {
				// 调用e.aof对象方法，保存命令
				e.aof.SaveRedisCommand(db.index, aof.Command(redisCommand))
			}
		}
	}
}

// 选中指定的 *DB
func (e *Engine) selectDB(index int) (*DB, *protocol.GenericErrReply) {
	if index < 0 || index >= len(e.dbSet) {
		return nil, protocol.NewGenericErrReply("db index is out of range")
	}
	return e.dbSet[index].Load().(*DB), nil
}

// redisCommand 待执行的命令  protocol.Reply 执行结果
func (e *Engine) Exec(c abstract.Connection, redisCommand [][]byte) (result protocol.Reply) {

	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = protocol.NewUnknownErrReply()
		}
	}()
	// 命令小写
	commandName := strings.ToLower(string(redisCommand[0]))
	if commandName == "ping" { // https://redis.io/commands/ping/
		return Ping(redisCommand[1:])
	}
	if commandName == "auth" { // https://redis.io/commands/auth/
		return Auth(c, redisCommand[1:])
	}
	// 校验密码
	if !checkPasswd(c) {
		return protocol.NewGenericErrReply("Authentication required")
	}

	// 基础命令
	switch commandName {
	case "select": // 表示当前连接，要选中哪个db https://redis.io/commands/select/
		if c != nil && c.IsTransaction() { // 事务模式，不能切换数据库
			return protocol.NewGenericErrReply("cannot select database within multi")
		}
		return execSelect(c, redisCommand[1:])
	case "bgrewriteaof": // https://redis.io/commands/bgrewriteaof/
		if !conf.GlobalConfig.AppendOnly {
			return protocol.NewGenericErrReply("AppendOnly is false, you can't rewrite aof file")
		}
		return BGRewriteAOF(e)
	case "subscribe":
		return e.hub.Subscribe(c, redisCommand[1:])
	case "unsubscribe":
		return e.hub.Unsubscribe(c, redisCommand[1:])
	case "publish":
		return e.hub.Publish(c, redisCommand[1:])
	}

	// redis 命令处理
	dbIndex := c.GetDBIndex()
	logger.Debugf("db index:%d", dbIndex)
	db, errReply := e.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	return db.Exec(c, redisCommand)
}

func (e *Engine) Close() {
	e.aof.Close()
}

func (e *Engine) RWLocks(dbIndex int, readKeys, writeKeys []string) {
	db, err := e.selectDB(dbIndex)
	if err != nil {
		logger.Error("RWLocks err:", err.Status)
		return
	}
	db.RWLock(readKeys, writeKeys)
}

func (e *Engine) RWUnLocks(dbIndex int, readKeys, writeKeys []string) {
	db, err := e.selectDB(dbIndex)
	if err != nil {
		logger.Error("RWLocks err:", err.Status)
		return
	}
	db.RWUnLock(readKeys, writeKeys)
}

func (e *Engine) GetUndoLogs(dbIndex int, redisCommand [][]byte) []CmdLine {
	db, err := e.selectDB(dbIndex)
	if err != nil {
		logger.Error("RWLocks err:", err.Status)
		return nil
	}
	return db.GetUndoLog(redisCommand)
}

func (e *Engine) ExecWithLock(dbIndex int, redisCommand [][]byte) protocol.Reply {
	db, err := e.selectDB(dbIndex)
	if err != nil {
		logger.Error("RWLocks err:", err.Status)
		return err
	}

	return db.execWithLock(redisCommand)
}

// 遍历引擎的所有数据
func (e *Engine) ForEach(dbIndex int, cb func(key string, data *payload.DataEntity, expiration *time.Time) bool) {

	db, errReply := e.selectDB(dbIndex)
	if errReply != nil {
		logger.Error("ForEach err ", errReply.ToBytes())
		return
	}

	db.dataDict.ForEach(func(key string, val interface{}) bool {
		entity, _ := val.(*payload.DataEntity)
		var expiration *time.Time
		rawExpireTime, ok := db.ttlDict.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}
		return cb(key, entity, expiration)
	})
}

func newAuxiliaryEngine() *Engine {
	engine := &Engine{}
	engine.delay = timewheel.NewDelay()
	engine.dbSet = make([]*atomic.Value, conf.GlobalConfig.Databases)
	for i := range engine.dbSet {

		db := newBasicDB(engine.delay)
		db.SetIndex(i)

		holder := &atomic.Value{}
		holder.Store(db)
		engine.dbSet[i] = holder
	}
	return engine
}
