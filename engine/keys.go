package engine

import "github.com/gofish2020/easyredis/redis/protocal"

func execDel(db *DB, args [][]byte) protocal.Reply {

	if len(args) < 1 {
		return protocal.NewArgNumErrReply("del")
	}

	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	deleted := db.Removes(keys...)
	return protocal.NewIntegerReply(deleted)
}

func init() {
	registerCommand("Del", execDel)
}
