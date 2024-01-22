package engine

import (
	"strconv"
	"strings"

	"github.com/gofish2020/easyredis/aof"
	"github.com/gofish2020/easyredis/datastruct/sortedset"
	"github.com/gofish2020/easyredis/engine/payload"
	"github.com/gofish2020/easyredis/redis/protocol"
)

// 获取有序集合对象
func (db *DB) getSortedSetObject(key string) (*sortedset.SortedSet, protocol.Reply) {

	payload, exist := db.GetEntity(key)
	if !exist { // 不存在
		return nil, nil
	}
	sortedSet, ok := payload.RedisObject.(*sortedset.SortedSet) // 该key的redis对象，不是集合
	if !ok {
		return nil, protocol.NewWrongTypeErrReply()
	}
	return sortedSet, nil // 有序集合对象
}

// 获取有序集合对象（如果不存在，则创建）
func (db *DB) getOrInitSortedSetObject(key string) (*sortedset.SortedSet, bool, protocol.Reply) {
	sortedSet, reply := db.getSortedSetObject(key)
	init := false
	if sortedSet == nil && reply == nil { //说明不存在该key的有序集合
		// 创建新的有序集合对象
		sortedSet = sortedset.NewSortedSet()
		db.PutEntity(key, &payload.DataEntity{
			RedisObject: sortedSet,
		})
		init = true
	}

	return sortedSet, init, reply
}

// zadd key score member [score member...]
func cmdZAdd(db *DB, args [][]byte) protocol.Reply {

	if len(args)%2 != 1 {
		return protocol.NewArgNumErrReply("ZAdd")
	}

	key := string(args[0])
	size := (len(args) - 1) / 2 // score/member 有多少对
	pairs := make([]*sortedset.Pair, size)

	for i := 0; i < size; i++ {
		score, err := strconv.ParseFloat(string(args[2*i+1]), 64)
		if err != nil {
			return protocol.NewGenericErrReply("value is not a valid float")
		}
		member := string(args[2*i+2])

		pairs[i] = &sortedset.Pair{
			Member: member,
			Score:  score,
		}
	}

	// 获取有序集合对象
	sortedSet, _, reply := db.getOrInitSortedSetObject(key)
	if reply != nil {
		return reply
	}

	// 将pairs保存到有序集合中
	i := int64(0)
	for _, pair := range pairs {
		if sortedSet.Add(pair.Member, pair.Score) {
			i++ // 新增的个数
		}
	}

	db.writeAof(aof.ZAddCmd(args...))
	return protocol.NewIntegerReply(i)
}

// zscore key member
func cmdZScore(db *DB, args [][]byte) protocol.Reply {

	if len(args) != 2 {
		return protocol.NewArgNumErrReply("ZScore")
	}
	key := string(args[0])
	member := string(args[1])
	sortedSet, reply := db.getSortedSetObject(key)
	if reply != nil {
		return reply
	}

	if sortedSet == nil {
		return protocol.NewNullBulkReply()
	}

	pair, exist := sortedSet.Get(member)
	if !exist {
		return protocol.NewNullBulkReply()
	}
	value := strconv.FormatFloat(pair.Score, 'f', -1, 64)
	return protocol.NewBulkReply([]byte(value))
}

// zrank key member [withscore]
func cmdZRank(db *DB, args [][]byte) protocol.Reply {
	if len(args) < 2 {
		return protocol.NewArgNumErrReply("ZRank")
	}
	key := string(args[0])
	member := string(args[1])
	sortedSet, reply := db.getSortedSetObject(key)
	if reply != nil { // key不存在 or key 类型错误
		return reply
	}

	if sortedSet == nil {
		return protocol.NewNullBulkReply()
	}

	rank := sortedSet.GetRank(member, false)
	if rank < 0 { // member不存在
		return protocol.NewNullBulkReply()
	}

	r := protocol.NewMixReply()
	r.Append(protocol.NewIntegerReply(rank)) // 索引从0开始
	if len(args) == 3 && strings.ToLower(string(args[2])) == "withscore" {
		pair, _ := sortedSet.Get(member)
		value := strconv.FormatFloat(pair.Score, 'f', -1, 64)
		r.Append(protocol.NewBulkReply([]byte(value)))
	}
	return r
}

func cmdZRevRank(db *DB, args [][]byte) protocol.Reply {
	if len(args) < 2 {
		return protocol.NewArgNumErrReply("ZRevRank")
	}
	key := string(args[0])
	member := string(args[1])
	sortedSet, reply := db.getSortedSetObject(key)
	if reply != nil { // key不存在 or key 类型错误
		return reply
	}

	if sortedSet == nil {
		return protocol.NewNullBulkReply()
	}

	rank := sortedSet.GetRank(member, true)
	if rank < 0 { // member不存在
		return protocol.NewNullBulkReply()
	}

	r := protocol.NewMixReply()
	r.Append(protocol.NewIntegerReply(rank)) // 索引从0开始
	if len(args) == 3 && strings.ToLower(string(args[2])) == "withscores" {
		pair, _ := sortedSet.Get(member)
		value := strconv.FormatFloat(pair.Score, 'f', -1, 64)
		r.Append(protocol.NewBulkReply([]byte(value)))
	}
	return r
}

// ZIncrBy key increment member
func cmdZIncrBy(db *DB, args [][]byte) protocol.Reply {
	key := string(args[0])
	rawDelta := string(args[1])
	member := string(args[2])
	delta, err := strconv.ParseFloat(rawDelta, 64)
	if err != nil {
		return protocol.NewGenericErrReply("value is not a valid float")
	}

	// get or init
	sortedSet, _, reply := db.getOrInitSortedSetObject(key)
	if reply != nil {
		return reply
	}

	pair, exist := sortedSet.Get(member)
	if !exist { // 不存在，就新增
		sortedSet.Add(member, delta)
		db.writeAof(aof.ZIncrByCmd(args...))
		return protocol.NewBulkReply(args[1]) // 返回分值
	}

	// 存在，就修改
	score := pair.Score + delta
	sortedSet.Add(member, score)
	value := strconv.FormatFloat(score, 'f', -1, 64)
	db.writeAof(aof.ZIncrByCmd(args...))
	return protocol.NewBulkReply([]byte(value)) // 返回分值
}

// zcount key min max    min/max 表示score的取值范围
func cmdZCount(db *DB, args [][]byte) protocol.Reply {

	if len(args) != 3 {
		return protocol.NewArgNumErrReply("ZCount")
	}

	min, err := sortedset.ParseScoreBorder(string(args[1])) // 解析最小分值
	if err != nil {
		return protocol.NewGenericErrReply(err.Error())
	}

	max, err := sortedset.ParseScoreBorder(string(args[2])) // 解析最大分值
	if err != nil {
		return protocol.NewGenericErrReply(err.Error())
	}

	key := string(args[0])
	sortedSet, reply := db.getSortedSetObject(key)
	if reply != nil { // 不存在
		return reply
	}

	if sortedSet == nil {
		return protocol.NewIntegerReply(0)
	}

	return protocol.NewIntegerReply(sortedSet.RangeCount(min, max))
}

// zcard key
func cmdZCard(db *DB, args [][]byte) protocol.Reply {

	if len(args) != 1 {
		return protocol.NewArgNumErrReply("ZCard")
	}

	key := string(args[0])
	sortedSet, errReply := db.getSortedSetObject(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.NewIntegerReply(0)
	}

	return protocol.NewIntegerReply(sortedSet.Len())

}

// zrange key start stop [withscore]， 这里的start和stop是包含的关系，而且可以为负数
func cmdZRange(db *DB, args [][]byte) protocol.Reply {
	if len(args) != 3 && len(args) != 4 {
		return protocol.NewArgNumErrReply("zrange")
	}

	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(string(args[3])) != "WITHSCORES" {
			return protocol.NewGenericErrReply("syntax error")
		}
		withScores = true
	}

	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.NewGenericErrReply("value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.NewGenericErrReply("value is not an integer or out of range")
	}
	return range0(db, key, start, stop, withScores, false)
}

// [start,stop]是包含边界
func range0(db *DB, key string, start int64, stop int64, withScores bool, desc bool) protocol.Reply {
	// get data
	sortedSet, errReply := db.getSortedSetObject(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.NewEmptyMultiBulkReply()
	}

	//将负数索引转成正数索引
	size := sortedSet.Len()

	if size == 0 {
		return protocol.NewEmptyMultiBulkReply()
	}

	if start < -1*size {
		start = 0
	} else if start < 0 { // -1 表示倒数第一个 -2表示倒数第二个
		start = size + start
	} else if start >= size { // 越界，返回空
		return protocol.NewEmptyMultiBulkReply()
	}

	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop
	} else if stop >= size {
		stop = size - 1
	}
	if stop < start {
		return protocol.NewEmptyMultiBulkReply()
	}
	stop = stop + 1

	// assert: start in [0, size - 1], stop in [start+1, size]
	// RangeByRank是左闭右开
	slice := sortedSet.RangeByRank(start, stop, desc)
	if withScores {
		result := make([][]byte, len(slice)*2) // 2倍的空间
		i := 0
		for _, element := range slice {
			result[i] = []byte(element.Member) // member
			i++
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = []byte(scoreStr) // score
			i++
		}
		return protocol.NewMultiBulkReply(result)
	}
	// 只返回 member
	result := make([][]byte, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = []byte(element.Member)
		i++
	}
	return protocol.NewMultiBulkReply(result)
}

func cmdZRevRange(db *DB, args [][]byte) protocol.Reply {

	// parse args
	if len(args) != 3 && len(args) != 4 {
		return protocol.NewArgNumErrReply("ZRevRange")
	}
	withScores := false
	if len(args) == 4 {
		if string(args[3]) != "WITHSCORES" {
			return protocol.NewGenericErrReply("syntax error")
		}
		withScores = true
	}
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.NewGenericErrReply("value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.NewGenericErrReply("value is not an integer or out of range")
	}
	return range0(db, key, start, stop, withScores, true)
}

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
func cmdZRangeByScore(db *DB, args [][]byte) protocol.Reply {
	if len(args) < 3 {
		return protocol.NewArgNumErrReply("ZRangeByScore")
	}
	key := string(args[0])

	// +inf inf -inf (1.0 1.0
	min, err := sortedset.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.NewGenericErrReply(err.Error())
	}
	// +inf inf -inf (1.0 1.0
	max, err := sortedset.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.NewGenericErrReply(err.Error())
	}

	withScores := false
	var offset int64 = 0
	var count int64 = -1
	if len(args) > 3 {
		// 扫面后面的可选参数
		for i := 3; i < len(args); {
			s := string(args[i])
			if strings.ToUpper(s) == "WITHSCORES" {
				withScores = true
				i++
			} else if strings.ToUpper(s) == "LIMIT" {
				if len(args) < i+3 {
					return protocol.NewGenericErrReply("syntax error")
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.NewGenericErrReply("value is not an integer or out of range")
				}
				count, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return protocol.NewGenericErrReply("value is not an integer or out of range")
				}
				i += 3
			} else {
				return protocol.NewGenericErrReply("syntax error")
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, count, withScores, false)
}

// count : A negative count returns all elements from the offset
func rangeByScore0(db *DB, key string, min sortedset.Border, max sortedset.Border, offset int64, count int64, withScores bool, desc bool) protocol.Reply {
	// get data
	sortedSet, errReply := db.getSortedSetObject(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.NewEmptyMultiBulkReply()
	}

	// 找到范围内的第一个节点，然后顺序扫描即可
	slice := sortedSet.Range(min, max, offset, count, desc)
	if withScores {
		result := make([][]byte, len(slice)*2)
		i := 0
		for _, element := range slice {
			result[i] = []byte(element.Member) // member
			i++
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = []byte(scoreStr) // score
			i++
		}
		return protocol.NewMultiBulkReply(result)
	}
	result := make([][]byte, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = []byte(element.Member)
		i++
	}
	return protocol.NewMultiBulkReply(result)
}

func cmdZRevRangeByScore(db *DB, args [][]byte) protocol.Reply {
	if len(args) < 3 {
		return protocol.NewArgNumErrReply("ZRevRangeByScore")
	}
	key := string(args[0])

	min, err := sortedset.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.NewGenericErrReply(err.Error())
	}

	max, err := sortedset.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.NewGenericErrReply(err.Error())
	}

	withScores := false
	var offset int64 = 0
	var count int64 = -1
	if len(args) > 3 {
		for i := 3; i < len(args); {
			s := string(args[i])
			if strings.ToUpper(s) == "WITHSCORES" {
				withScores = true
				i++
			} else if strings.ToUpper(s) == "LIMIT" {
				if len(args) < i+3 {
					return protocol.NewGenericErrReply("syntax error")
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.NewGenericErrReply("value is not an integer or out of range")
				}
				count, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return protocol.NewGenericErrReply("value is not an integer or out of range")
				}
				i += 3
			} else {
				return protocol.NewGenericErrReply("syntax error")
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, count, withScores, true)
}

// Removes and returns up to count members with the lowest scores in the sorted set stored at key. the default value for count is 1
// ZPOPMIN key [count]
func cmdZPopMin(db *DB, args [][]byte) protocol.Reply {

	// 获取key
	key := string(args[0])
	// 默认1
	count := 1
	if len(args) > 1 {
		var err error
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return protocol.NewGenericErrReply("value is not an integer or out of range")
		}
	}

	sortedSet, errReply := db.getSortedSetObject(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.NewEmptyMultiBulkReply()
	}

	removed := sortedSet.PopMin(count)
	if len(removed) > 0 {
		db.writeAof(aof.ZPopMin(args...))
	}
	result := make([][]byte, 0, len(removed)*2)
	for _, element := range removed {
		scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
		result = append(result, []byte(element.Member), []byte(scoreStr))
	}
	return protocol.NewMultiBulkReply(result)
}

// ZREM key member [member ...]
func cmdZRem(db *DB, args [][]byte) protocol.Reply {
	// parse args
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}

	// get entity
	sortedSet, errReply := db.getSortedSetObject(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.NewIntegerReply(0)
	}

	var deleted int64 = 0
	for _, field := range fields {
		if sortedSet.Remove(field) {
			deleted++
		}
	}
	if deleted > 0 {
		db.writeAof(aof.ZRem(args...))
	}
	return protocol.NewIntegerReply(deleted)
}

// ZREMRANGEBYSCORE key min max
func cmdZRemRangeByScore(db *DB, args [][]byte) protocol.Reply {
	if len(args) != 3 {
		return protocol.NewArgNumErrReply("ZRemRangeByScore")
	}

	key := string(args[0])
	min, err := sortedset.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.NewGenericErrReply(err.Error())
	}

	max, err := sortedset.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.NewGenericErrReply(err.Error())
	}

	// 有序集合对象
	sortedSet, errReply := db.getSortedSetObject(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.NewIntegerReply(0)
	}

	removed := sortedSet.RemoveRange(min, max)
	if removed > 0 {
		db.writeAof(aof.ZRemRangeByScore(args...))
	}
	return protocol.NewIntegerReply(removed)
}

// ZREMRANGEBYRANK key start stop  [start,stop]边界包含
func cmdZRemRangeByRank(db *DB, args [][]byte) protocol.Reply {

	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.NewGenericErrReply("value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.NewGenericErrReply("value is not an integer or out of range")
	}

	sortedSet, errReply := db.getSortedSetObject(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.NewIntegerReply(0)
	}

	// 索引转化
	size := sortedSet.Len()
	if size == 0 {
		return protocol.NewIntegerReply(0)
	}
	if start < -1*size {
		start = 0
	} else if start < 0 { // 负数转正数
		start = size + start
	} else if start >= size {
		return protocol.NewIntegerReply(0)
	}

	if stop < -1*size {
		stop = 0
	} else if stop < 0 { // 负数转正数
		stop = size + stop
	} else if stop >= size {
		stop = size - 1
	}
	if stop < start {
		return protocol.NewIntegerReply(0)
	}
	stop = stop + 1

	// 左闭右开
	removed := sortedSet.RemoveByRank(start, stop)
	if removed > 0 {
		db.writeAof(aof.ZRemRangeByRank(args...))
	}
	return protocol.NewIntegerReply(removed)
}

func undoZAdd(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	size := (len(args) - 1) / 2
	members := make([]string, size)
	for i := 0; i < size; i++ {
		members[i] = string(args[2*i+2])
	}
	return rollbackZSetMembers(db, key, members...)
}

func undoZIncr(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	member := string(args[2])
	return rollbackZSetMembers(db, key, member)
}

func undoZRem(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	members := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		members[i] = string(v)
	}
	return rollbackZSetMembers(db, key, members...)
}
func init() {
	// 新增 zadd key score member
	registerCommand("ZAdd", cmdZAdd, writeFirstKey, -4, undoZAdd)
	// 获取member 分值 zscore key member
	registerCommand("ZScore", cmdZScore, readFirstKey, 3, nil)
	// 增加分值 zincrby key delta member
	registerCommand("ZIncrBy", cmdZIncrBy, writeFirstKey, 4, undoZIncr)
	// 获取member索引 zrank key member
	registerCommand("ZRank", cmdZRank, readFirstKey, 3, nil)       //正序索引
	registerCommand("ZRevRank", cmdZRevRank, readFirstKey, 3, nil) //倒序索引
	// 分值范围内，member个数 zcount key -inf inf
	registerCommand("ZCount", cmdZCount, readFirstKey, 4, nil)

	// 总共有多少元素 zcard key
	registerCommand("ZCard", cmdZCard, readFirstKey, 2, nil)

	// 根据[索引]扫描链表内元素 [start,stop] zrange key 0 1 [withscores]
	registerCommand("ZRange", cmdZRange, readFirstKey, -4, nil)       // 正序元素
	registerCommand("ZRevRange", cmdZRevRange, readFirstKey, -4, nil) // 倒序元素

	// 根据【分值】扫描链表 ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
	registerCommand("ZRangeByScore", cmdZRangeByScore, readFirstKey, -4, nil)       // 正序
	registerCommand("ZRevRangeByScore", cmdZRevRangeByScore, readFirstKey, -4, nil) // 倒序

	// 删除最小分值 ZPOPMIN key [count]
	registerCommand("ZPopMin", cmdZPopMin, writeFirstKey, -2, rollbackFirstKey)

	// 删除member ZREM key member [member ...]
	registerCommand("ZRem", cmdZRem, writeFirstKey, -3, undoZRem)

	// 范围删除 ZREMRANGEBYSCORE key min max
	registerCommand("ZRemRangeByScore", cmdZRemRangeByScore, writeFirstKey, 4, rollbackFirstKey) // 利用分值删除
	registerCommand("ZRemRangeByRank", cmdZRemRangeByRank, writeFirstKey, 4, rollbackFirstKey)   //利用索引删除

}
