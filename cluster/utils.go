package cluster

import "strconv"

// 计算key应该存储的节点ip
func (cluster *Cluster) groupByKeys(keys []string) map[string][]string {
	var result = make(map[string][]string)
	for _, key := range keys {
		ip := cluster.consistHash.Get(key)
		result[ip] = append(result[ip], key)
	}
	return result
}

// 替换命令名

func replaceCmd(redisCommand [][]byte, newCmd string) [][]byte {
	newRedisCommand := make([][]byte, len(redisCommand))
	copy(newRedisCommand, redisCommand)
	newRedisCommand[0] = []byte(newCmd)
	return newRedisCommand
}

// 头部添加命令名
func pushCmd(redisCommand [][]byte, newCmd string) [][]byte {
	result := make([][]byte, len(redisCommand)+1)
	result[0] = []byte(newCmd)
	for i := 0; i < len(redisCommand); i++ {
		result[i+1] = []byte(redisCommand[i])
	}
	return result
}

// 删除头部的命令
func popCmd(redisCommand [][]byte) [][]byte {
	result := make([][]byte, len(redisCommand)-1)
	copy(result, redisCommand[1:])
	return result
}

// 生成事务id
func (cluster *Cluster) newTxId() string {
	id := cluster.snowflake.NextID()
	return strconv.FormatInt(id, 10)
}
