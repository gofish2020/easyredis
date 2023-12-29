package engine

import (
	"strconv"
	"strings"

	"github.com/gofish2020/easyredis/engine/payload"
	"github.com/gofish2020/easyredis/redis/protocal"
)

const (
	defaultPolicy = iota + 1 // 插入 or 更新
	insertPolicy             // 只插入
	updatePolicy             // 只更新

)

const nolimitedTTL int64 = 0 // 过期时间

// 获取底层存储对象【字节流】
func (db *DB) getStringObject(key string) ([]byte, protocal.Reply) {
	payload, exist := db.GetEntity(key)
	if !exist {
		return nil, protocal.NewNullBulkReply()
	}
	// 判断底层对象是否为【字节流】
	bytes, ok := payload.RedisObject.([]byte)
	if !ok {
		return nil, protocal.NewWrongTypeErrReply()
	}
	return bytes, nil
}

// https://redis.io/commands/get/     key
func cmdGet(db *DB, args [][]byte) protocal.Reply {
	if len(args) != 1 {
		return protocal.NewSyntaxErrReply()
	}

	key := string(args[0])
	bytes, reply := db.getStringObject(key)
	if reply != nil {
		return reply
	}
	return protocal.NewBulkReply(bytes)
}

// https://redis.io/commands/set/      key value nx/xx ex/px 60
func cmdSet(db *DB, args [][]byte) protocal.Reply {
	key := string(args[0])
	value := args[1]

	policy := defaultPolicy
	ttl := nolimitedTTL
	if len(args) > 2 {

		for i := 2; i < len(args); i++ {
			arg := strings.ToUpper(string(args[i]))
			if arg == "NX" { // 插入
				if policy == updatePolicy { // 说明policy 已经被设置过，重复设置（语法错误）
					return protocal.NewSyntaxErrReply()
				}
				policy = insertPolicy
			} else if arg == "XX" { // 更新
				if policy == insertPolicy { // 说明policy 已经被设置过，重复设置（语法错误）
					return protocal.NewSyntaxErrReply()
				}
				policy = updatePolicy
			} else if arg == "EX" { // ex in seconds

				if ttl != nolimitedTTL { // 说明  ttl 已经被设置过，重复设置（语法错误）
					return protocal.NewSyntaxErrReply()
				}
				if i+1 >= len(args) { // 过期时间后面要跟上正整数
					return protocal.NewSyntaxErrReply()
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocal.NewSyntaxErrReply()
				}

				if ttlArg <= 0 {
					return protocal.NewGenericErrReply("expire time is not a positive integer")
				}
				// 转成 ms
				ttl = ttlArg * 1000
				i++ // 跳过下一个参数
			} else if arg == "PX" { // px in milliseconds

				if ttl != nolimitedTTL { // 说明  ttl 已经被设置过，重复设置（语法错误）
					return protocal.NewSyntaxErrReply()
				}
				if i+1 >= len(args) { // 过期时间后面要跟上正整数
					return protocal.NewSyntaxErrReply()
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocal.NewSyntaxErrReply()
				}

				if ttlArg <= 0 {
					return protocal.NewGenericErrReply("expire time is not a positive integer")
				}

				ttl = ttlArg
				i++ //跳过下一个参数
			} else {
				// 发现不符合要求的参数
				return protocal.NewSyntaxErrReply()
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

	if ttl != nolimitedTTL {
		//TODO： 过期时间处理
	}

	if result > 0 {
		return protocal.NewOkReply()
	}

	return protocal.NewNullBulkReply()
}
func init() {
	registerCommand("Get", cmdGet)
	registerCommand("Set", cmdSet)
}
