package engine

import (
	"strconv"
	"strings"
	"time"

	"github.com/gofish2020/easyredis/aof"
	"github.com/gofish2020/easyredis/engine/payload"
	"github.com/gofish2020/easyredis/redis/protocol"
)

const (
	defaultPolicy = iota + 1 // 插入 or 更新
	insertPolicy             // 只插入
	updatePolicy             // 只更新

)

const nolimitedTTL int64 = 0 // 过期时间

// 获取底层存储对象【字节流】
func (db *DB) getStringObject(key string) ([]byte, protocol.Reply) {
	payload, exist := db.GetEntity(key)
	if !exist {
		return nil, protocol.NewNullBulkReply()
	}
	// 判断底层对象是否为【字节流】
	bytes, ok := payload.RedisObject.([]byte)
	if !ok {
		return nil, protocol.NewWrongTypeErrReply()
	}
	return bytes, nil
}

// https://redis.io/commands/get/     key
func cmdGet(db *DB, args [][]byte) protocol.Reply {
	if len(args) != 1 {
		return protocol.NewSyntaxErrReply()
	}

	key := string(args[0])
	bytes, reply := db.getStringObject(key)
	if reply != nil {
		return reply
	}
	return protocol.NewBulkReply(bytes)
}

// https://redis.io/commands/set/      key value nx/xx ex/px 60
func cmdSet(db *DB, args [][]byte) protocol.Reply {
	key := string(args[0])
	value := args[1]

	policy := defaultPolicy
	ttl := nolimitedTTL
	if len(args) > 2 {

		for i := 2; i < len(args); i++ {
			arg := strings.ToUpper(string(args[i]))
			if arg == "NX" { // 插入
				if policy == updatePolicy { // 说明policy 已经被设置过，重复设置（语法错误）
					return protocol.NewSyntaxErrReply()
				}
				policy = insertPolicy
			} else if arg == "XX" { // 更新
				if policy == insertPolicy { // 说明policy 已经被设置过，重复设置（语法错误）
					return protocol.NewSyntaxErrReply()
				}
				policy = updatePolicy
			} else if arg == "EX" { // ex in seconds

				if ttl != nolimitedTTL { // 说明  ttl 已经被设置过，重复设置（语法错误）
					return protocol.NewSyntaxErrReply()
				}
				if i+1 >= len(args) { // 过期时间后面要跟上正整数
					return protocol.NewSyntaxErrReply()
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.NewSyntaxErrReply()
				}

				if ttlArg <= 0 {
					return protocol.NewGenericErrReply("expire time is not a positive integer")
				}
				// 转成 ms
				ttl = ttlArg * 1000
				i++ // 跳过下一个参数
			} else if arg == "PX" { // px in milliseconds

				if ttl != nolimitedTTL { // 说明  ttl 已经被设置过，重复设置（语法错误）
					return protocol.NewSyntaxErrReply()
				}
				if i+1 >= len(args) { // 过期时间后面要跟上正整数
					return protocol.NewSyntaxErrReply()
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.NewSyntaxErrReply()
				}

				if ttlArg <= 0 {
					return protocol.NewGenericErrReply("expire time is not a positive integer")
				}

				ttl = ttlArg
				i++ //跳过下一个参数
			} else {
				// 发现不符合要求的参数
				return protocol.NewSyntaxErrReply()
			}
		}
	}

	// 构建存储实体
	entity := payload.DataEntity{
		RedisObject: value,
	}

	// 保存到内存字典中
	var result int
	if policy == defaultPolicy {
		db.PutEntity(key, &entity)
		result = 1
	} else if policy == insertPolicy {
		result = db.PutIfAbsent(key, &entity)
	} else if policy == updatePolicy {
		result = db.PutIfExist(key, &entity)
	}

	if result > 0 { // 1 表示存储成功
		//TODO： 过期时间处理
		if ttl != nolimitedTTL { // 设定key过期
			expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			db.ExpireAt(key, expireTime)
			//写入日志
			db.writeAof(aof.SetCmd([][]byte{args[0], args[1]}...))
			db.writeAof(aof.PExpireAtCmd(string(args[0]), expireTime))
		} else { // 设定key不过期
			db.Persist(key)
			//写入日志
			db.writeAof(aof.SetCmd(args...))
		}
		return protocol.NewOkReply()
	}

	return protocol.NewNullBulkReply()
}

func cmdMSet(db *DB, args [][]byte) protocol.Reply {
	size := len(args) / 2
	// 提取出key value
	keys := make([]string, 0, size)
	values := make([][]byte, 0, size)
	for i := 0; i < size; i++ {
		keys = append(keys, string(args[2*i]))
		values = append(values, args[2*i+1])
	}
	// 保存到内存中
	for i, key := range keys {
		value := values[i]
		entity := payload.DataEntity{
			RedisObject: value,
		}
		db.PutEntity(key, &entity)
	}
	// 写日志
	db.writeAof(aof.MSetCmd(args...))
	return protocol.NewOkReply()
}
func init() {
	// 获取值
	registerCommand("Get", cmdGet, readFirstKey, 2, nil)
	// 设置值
	registerCommand("Set", cmdSet, writeFirstKey, -3, rollbackFirstKey)
	// 设置多个值
	registerCommand("MSet", cmdMSet, writeMultiKey, -3, undoMSet)
}
