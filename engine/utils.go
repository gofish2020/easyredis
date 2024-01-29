package engine

import (
	"strconv"

	"github.com/gofish2020/easyredis/aof"
)

func readFirstKey(args [][]byte) ([]string, []string) {
	return []string{string(args[0])}, nil
}

func writeFirstKey(args [][]byte) ([]string, []string) {
	return nil, []string{string(args[0])}
}

func readAllKey(args [][]byte) ([]string, []string) {
	readKeys := make([]string, len(args))
	for i, arg := range args {
		readKeys[i] = string(arg)
	}
	return readKeys, nil
}

func writeAllKey(args [][]byte) ([]string, []string) {
	writeKeys := make([]string, len(args))
	for i, arg := range args {
		writeKeys[i] = string(arg)
	}
	return nil, writeKeys
}

func noKey(args [][]byte) ([]string, []string) {
	return nil, nil
}

func writeMultiKey(args [][]byte) ([]string, []string) {

	size := len(args) / 2

	writeKeys := make([]string, size)

	for i := 0; i < size; i++ {
		writeKeys = append(writeKeys, string(args[2*i]))
	}

	return nil, writeKeys
}

// ********* 回滚 ***********

// 通用的回滚（其实就是将整个内存数据都记录下来）
func rollbackFirstKey(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	return rollbackGivenKeys(db, key)
}

func rollbackGivenKeys(db *DB, keys ...string) []CmdLine {
	var undoCmdLines []CmdLine
	for _, key := range keys {
		// 获取内存对象
		entity, ok := db.GetEntity(key)
		if !ok {
			undoCmdLines = append(undoCmdLines,
				aof.Del([]byte(key)), // key不存在，del
			)
		} else {
			undoCmdLines = append(undoCmdLines,
				aof.Del([]byte(key)),                      // 先清理
				aof.EntityToCmd(key, entity).RedisCommand, // redis命令
				toTTLCmd(db, key).RedisCommand,            // 过期
			)
		}
	}
	return undoCmdLines
}

func rollbackZSetMembers(db *DB, key string, members ...string) []CmdLine {
	var undoCmdLines [][][]byte
	// 获取有序集合对象
	zset, errReply := db.getSortedSetObject(key)
	if errReply != nil {
		return nil
	}
	// 说明集合对象不存在（所以要生成删除回滚）
	if zset == nil {
		undoCmdLines = append(undoCmdLines,
			aof.Del([]byte(key)),
		)
		return undoCmdLines
	}
	for _, member := range members {
		elem, ok := zset.Get(member)
		if !ok { // member不存在(回滚：就是删除)
			undoCmdLines = append(undoCmdLines,
				aof.ZRem([]byte(key), []byte(member)),
			)
		} else {
			// 记录原始值
			score := strconv.FormatFloat(elem.Score, 'f', -1, 64)
			undoCmdLines = append(undoCmdLines,
				aof.ZAddCmd([]byte(key), []byte(score), []byte(member)),
			)
		}
	}
	return undoCmdLines
}

func undoMSet(db *DB, args [][]byte) []CmdLine {
	_, writeKeys := writeMultiKey(args)
	return rollbackGivenKeys(db, writeKeys...)
}
