package engine

import (
	"strconv"
	"strings"
	"time"

	"github.com/gofish2020/easyredis/aof"
	"github.com/gofish2020/easyredis/redis/protocol"
	"github.com/gofish2020/easyredis/tool/wildcard"
	"github.com/gofish2020/easyredis/utils"
)

func execDel(db *DB, args [][]byte) protocol.Reply {

	if len(args) < 1 {
		return protocol.NewArgNumErrReply("del")
	}

	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	deleted := db.Removes(keys...)
	// 删除写入日志
	if deleted > 0 {
		db.writeAof(utils.BuildCmdLine("del", args...))
	}
	return protocol.NewIntegerReply(deleted)
}

func execExpire(db *DB, args [][]byte) protocol.Reply {

	if len(args) < 2 || len(args) > 3 {
		return protocol.NewArgNumErrReply("Expire")
	}

	key := string(args[0])

	seconds, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || seconds < 0 {
		return protocol.NewGenericErrReply("argument seconds is not invalue")
	}
	// 如果key不存在
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.NewIntegerReply(0)
	}
	/*
		NX -- Set expiry only when the key has no expiry
		XX -- Set expiry only when the key has an existing expiry
		GT -- Set expiry only when the new expiry is greater than current one
		LT -- Set expiry only when the new expiry is less than current one
	*/
	if len(args) == 3 {
		policyStr := strings.ToLower(string(args[2]))
		switch policyStr {
		case "xx":
			if db.IsPersist(key) {
				return protocol.NewIntegerReply(0)
			}
		case "nx":
			if !db.IsPersist(key) {
				return protocol.NewIntegerReply(0)
			}
		case "gt":
			if seconds < db.TTL(key) {
				return protocol.NewIntegerReply(0)
			}
		case "lt":
			if seconds > db.TTL(key) {
				return protocol.NewIntegerReply(0)
			}
		}
	}

	expireAt := time.Now().Add(time.Duration(seconds) * time.Second)
	db.ExpireAt(key, expireAt)
	db.writeAof(aof.PExpireAtCmd(key, expireAt))
	return protocol.NewIntegerReply(1)
}

func execTTL(db *DB, args [][]byte) protocol.Reply {

	if len(args) != 1 {
		return protocol.NewArgNumErrReply("ttl")
	}
	// 不存在返回-2
	key := string(args[0])
	_, exist := db.GetEntity(key)
	if !exist {
		return protocol.NewIntegerReply(-2)
	}

	ttl := db.TTL(key)
	return protocol.NewIntegerReply(ttl)
}

func execPTTL(db *DB, args [][]byte) protocol.Reply {
	if len(args) != 1 {
		return protocol.NewArgNumErrReply("ttl")
	}
	// 不存在返回-2
	key := string(args[0])
	_, exist := db.GetEntity(key)
	if !exist {
		return protocol.NewIntegerReply(-2)
	}

	ttl := db.PTTL(key)
	return protocol.NewIntegerReply(ttl)
}

func execPersist(db *DB, args [][]byte) protocol.Reply {
	if len(args) != 1 {
		return protocol.NewArgNumErrReply("persist")
	}
	// key不存在 or key本身就没有过期时间
	key := string(args[0])
	_, exist := db.GetEntity(key)
	if !exist || db.IsPersist(key) {
		return protocol.NewIntegerReply(0)
	}
	// 将key设定为永久存在
	db.Persist(key)
	db.writeAof(utils.BuildCmdLine("persist", args...))
	return protocol.NewIntegerReply(1)
}

func execExists(db *DB, args [][]byte) protocol.Reply {

	var result int64 = 0
	for _, key := range args {

		_, exist := db.GetEntity(string(key))
		if exist {
			result++
		}
	}
	return protocol.NewIntegerReply(result)
}

func execPExpireAt(db *DB, args [][]byte) protocol.Reply {

	key := string(args[0])

	timestamp := string(args[1])
	i, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return protocol.NewGenericErrReply("value is not an integer or out of range")
	}
	_, exist := db.GetEntity(key)
	if !exist {
		return protocol.NewIntegerReply(0)
	}

	expireAt := time.Unix(0, i*int64(time.Millisecond))
	db.ExpireAt(key, expireAt)
	db.writeAof(aof.PExpireAtCmd(key, expireAt))
	return protocol.NewIntegerReply(1)
}

func execExpireAt(db *DB, args [][]byte) protocol.Reply {
	key := string(args[0])

	timestamp := string(args[1])
	i, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return protocol.NewGenericErrReply("value is not an integer or out of range")
	}
	_, exist := db.GetEntity(key)
	if !exist {
		return protocol.NewIntegerReply(0)
	}

	expireAt := time.Unix(i, 0)
	db.ExpireAt(key, expireAt)
	db.writeAof(aof.PExpireAtCmd(key, expireAt))
	return protocol.NewIntegerReply(1)
}

func execExpireTime(db *DB, args [][]byte) protocol.Reply {
	if len(args) != 1 {
		return protocol.NewArgNumErrReply("ExpireTime")
	}
	key := string(args[0])
	_, exist := db.GetEntity(key)
	if !exist {
		return protocol.NewIntegerReply(-2)
	}

	stamp := db.ExpireTime(key)
	return protocol.NewIntegerReply(stamp)
}

func execPExpireTime(db *DB, args [][]byte) protocol.Reply {
	if len(args) != 1 {
		return protocol.NewArgNumErrReply("PExpireTime")
	}
	key := string(args[0])
	_, exist := db.GetEntity(key)
	if !exist {
		return protocol.NewIntegerReply(-2)
	}
	stamp := db.PExpireTime(key)
	return protocol.NewIntegerReply(stamp)
}

func execPExpire(db *DB, args [][]byte) protocol.Reply {

	if len(args) < 2 || len(args) > 3 {
		return protocol.NewArgNumErrReply("PExpire")
	}

	key := string(args[0])

	milliseconds, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || milliseconds < 0 {
		return protocol.NewGenericErrReply("argument milliseconds is not invalue")
	}
	// 如果key不存在
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.NewIntegerReply(0)
	}
	/*
		NX -- Set expiry only when the key has no expiry
		XX -- Set expiry only when the key has an existing expiry
		GT -- Set expiry only when the new expiry is greater than current one
		LT -- Set expiry only when the new expiry is less than current one
	*/
	if len(args) == 3 {
		policyStr := strings.ToLower(string(args[2]))
		switch policyStr {
		case "xx":
			if db.IsPersist(key) {
				return protocol.NewIntegerReply(0)
			}
		case "nx":
			if !db.IsPersist(key) {
				return protocol.NewIntegerReply(0)
			}
		case "gt":
			if milliseconds < db.PTTL(key) {
				return protocol.NewIntegerReply(0)
			}
		case "lt":
			if milliseconds > db.PTTL(key) {
				return protocol.NewIntegerReply(0)
			}
		}
	}

	expireAt := time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
	db.ExpireAt(key, expireAt)
	db.writeAof(aof.PExpireAtCmd(key, expireAt))
	return protocol.NewIntegerReply(1)
}

// 在当前数据库中，找匹配的key
func execKeys(db *DB, args [][]byte) protocol.Reply {
	pattern, err := wildcard.CompilePattern(string(args[0]))
	if err != nil {
		return protocol.NewGenericErrReply("illegal wildcard")
	}
	result := [][]byte{}
	db.dataDict.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) && !db.IsExpire(key) {
			result = append(result, []byte(key))
		}
		return true
	})

	return protocol.NewMultiBulkReply(result)
}
func init() {
	// 删除
	registerCommand("Del", execDel)
	// 设置过期 s
	registerCommand("Expire", execExpire)
	// 设定过期 ms
	registerCommand("PExpire", execPExpire)
	// 获取过期时间戳(Unix timestamp) s
	registerCommand("ExpireTime", execExpireTime)
	// 获取过期时间戳(Unix timestamp) ms
	registerCommand("PExpireTime", execPExpireTime)
	// 设定过期（时间戳 Unix timestamp）ms
	registerCommand("PExpireAt", execPExpireAt)
	// 设定过期（时间戳 Unix timestamp）s
	registerCommand("ExpireAt", execExpireAt)
	// 设置永不过期
	registerCommand("Persist", execPersist)
	// 获取剩余过期时间(s)
	registerCommand("TTL", execTTL)
	// 获取剩余过期时间(ms)
	registerCommand("PTTL", execPTTL)
	// 判断key是否存在
	registerCommand("Exists", execExists)
	// 获取所有的key
	registerCommand("Keys", execKeys)
}
