package aof

import (
	"strconv"
	"time"

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

func SelectCmd(args ...[]byte) [][]byte {
	return utils.BuildCmdLine("SELECT", args...)
}

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
		// case *SortedSet.SortedSet:
		// 	cmd = zSetToCmd(key, val)
	}
	return cmd
}
