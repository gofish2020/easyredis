package engine

import (
	"strconv"
	"strings"
	"time"

	"github.com/gofish2020/easyredis/redis/protocol"
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
	db.Expire(key, expireAt)
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
	return protocol.NewIntegerReply(1)
}
func init() {
	// 删除
	registerCommand("Del", execDel)
	// 设置过期
	registerCommand("Expire", execExpire)
	// 设置永不过期
	registerCommand("Persist", execPersist)
	// 获取剩余过期时间
	registerCommand("TTL", execTTL)
}
