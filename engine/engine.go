package engine

import (
	"fmt"
	"runtime/debug"
	"strings"
	"sync/atomic"

	"github.com/gofish2020/easyredis/redis/connection"
	"github.com/gofish2020/easyredis/redis/protocal"
	"github.com/gofish2020/easyredis/tool/conf"
	"github.com/gofish2020/easyredis/tool/logger"
)

// 存储引擎，负责数据的CRUD
type Engine struct {
	dbSet []*atomic.Value // *DB
}

func NewEngine() *Engine {

	engine := &Engine{}
	// 多个dbSet
	engine.dbSet = make([]*atomic.Value, conf.GlobalConfig.Databases)
	for i := 0; i < conf.GlobalConfig.Databases; i++ {
		// 创建 *db
		db := newDB()
		db.SetIndex(i)
		// 保存到 atomic.Value中
		dbset := &atomic.Value{}
		dbset.Store(db)
		// 赋值到 dbSet中
		engine.dbSet[i] = dbset
	}

	return engine
}

// 选中指定的 *DB
func (e *Engine) selectDB(index int) (*DB, *protocal.GenericErrReply) {
	if index < 0 || index >= len(e.dbSet) {
		return nil, protocal.NewGenericErrReply("db index is out of range")
	}

	return e.dbSet[index].Load().(*DB), nil
}

// redisCommand 待执行的命令  protocal.Reply 执行结果
func (e *Engine) Exec(c *connection.KeepConnection, redisCommand [][]byte) (result protocal.Reply) {

	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = protocal.NewUnknownErrReply()
		}
	}()
	commandName := strings.ToLower(string(redisCommand[0]))
	if commandName == "ping" { // https://redis.io/commands/ping/
		return Ping(redisCommand[1:])
	}
	if commandName == "auth" { // https://redis.io/commands/auth/
		return Auth(c, redisCommand[1:])
	}
	// 校验密码
	if !checkPasswd(c) {
		return protocal.NewGenericErrReply("Authentication required")
	}

	// 基础命令
	switch commandName {
	case "select": // 表示当前连接，要选中哪个db https://redis.io/commands/select/
		return execSelect(c, redisCommand[1:])
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
