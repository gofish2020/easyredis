package aof

import (
	"strconv"
	"time"

	"github.com/gofish2020/easyredis/datastruct/sortedset"
	"github.com/gofish2020/easyredis/engine/payload"
	"github.com/gofish2020/easyredis/redis/protocol"
	"github.com/gofish2020/easyredis/utils"
)

// 指定key绝对时间戳过期 milliseconds
func PExpireAtCmd(key string, expireAt time.Time) [][]byte {
	return utils.BuildCmdLine("PEXPIREAT", [][]byte{[]byte(key), []byte(strconv.FormatInt((expireAt.UnixNano() / 1e6), 10))}...)
}

func SetCmd(args ...[]byte) [][]byte {
	return utils.BuildCmdLine("SET", args...)
}

func MSetCmd(args ...[]byte) [][]byte {
	return utils.BuildCmdLine("MSet", args...)
}

func SelectCmd(args ...[]byte) [][]byte {
	return utils.BuildCmdLine("SELECT", args...)
}

func ZAddCmd(args ...[]byte) [][]byte {
	return utils.BuildCmdLine("ZADD", args...)
}

// ZIncrBy
func ZIncrByCmd(args ...[]byte) [][]byte {
	return utils.BuildCmdLine("ZINCRBY", args...)
}

// zpopmin
func ZPopMin(args ...[]byte) [][]byte {
	return utils.BuildCmdLine("ZPOPMIN", args...)
}

// ZRem
func ZRem(args ...[]byte) [][]byte {
	return utils.BuildCmdLine("ZREM", args...)
}

// zremrangebyscore
func ZRemRangeByScore(args ...[]byte) [][]byte {
	return utils.BuildCmdLine("ZREMRANGEBYSCORE", args...)
}

// zremrangebyrank
func ZRemRangeByRank(args ...[]byte) [][]byte {
	return utils.BuildCmdLine("ZREMRANGEBYRANK", args...)
}

// del
func Del(args ...[]byte) [][]byte {
	return utils.BuildCmdLine("DEL", args...)
}

// persist
func Persist(args ...[]byte) [][]byte {
	return utils.BuildCmdLine("PERSIST", args...)
}

// Auth
func Auth(args ...[]byte) [][]byte {
	return utils.BuildCmdLine("AUTH", args...)
}

// Get

func Get(args ...[]byte) [][]byte {
	return utils.BuildCmdLine("GET", args...)
}

// 内存对象转换成 redis命令
func EntityToCmd(key string, entity *payload.DataEntity) *protocol.MultiBulkReply {
	if entity == nil {
		return nil
	}
	var cmd *protocol.MultiBulkReply
	switch val := entity.RedisObject.(type) {
	case []byte:
		cmd = protocol.NewMultiBulkReply(SetCmd([]byte(key), val))
	// case List.List:
	// 	cmd = listToCmd(key, val)
	// case *set.Set:
	// 	cmd = setToCmd(key, val)
	// case dict.Dict:
	// 	cmd = hashToCmd(key, val)
	case *sortedset.SortedSet:
		cmd = zSetToCmd(key, val)
	}
	return cmd
}

func zSetToCmd(key string, set *sortedset.SortedSet) *protocol.MultiBulkReply {
	size := set.Len()
	args := make([][]byte, 1+2*size)
	args[0] = []byte(key) // key
	i := 0

	set.ForEachByRank(0, size, true, func(pair *sortedset.Pair) bool {
		score := strconv.FormatFloat(pair.Score, 'f', -1, 64)
		args[2*i+1] = []byte(score)       // score
		args[2*i+2] = []byte(pair.Member) // member
		i++
		return true
	})
	// zadd key score member [score member..]
	return protocol.NewMultiBulkReply(ZAddCmd(args...))
}
