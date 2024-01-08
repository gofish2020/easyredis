package pubhub

import (
	"strconv"

	"github.com/gofish2020/easyredis/abstract"
	"github.com/gofish2020/easyredis/datastruct/dict"
	"github.com/gofish2020/easyredis/datastruct/list"
	"github.com/gofish2020/easyredis/redis/protocol"
	"github.com/gofish2020/easyredis/tool/locker"
	"github.com/gofish2020/easyredis/tool/logger"
	"github.com/gofish2020/easyredis/utils"
)

/*
发布订阅的底层数据结构： map + list
map中的key表示channel， list记录订阅该channel的客户端
*/

const (
	_subscribe   = "subscribe"
	_unsubscribe = "unsubscribe"
	_message     = "message"
)

// https://redis.io/docs/interact/pubsub/
// chanName 通道名   count总共订阅成功的数量
func channelMsg(action, chanName string, count int) []byte {
	// Wire protocol
	return []byte("*3" + utils.CRLF +
		"$" + strconv.Itoa(len(action)) + utils.CRLF + action + utils.CRLF +
		"$" + strconv.Itoa(len(chanName)) + utils.CRLF + chanName + utils.CRLF +
		":" + strconv.Itoa(count) + utils.CRLF)
}

func noChannelMsg() []byte {
	return []byte("*3" + utils.CRLF +
		"$" + strconv.Itoa(len(_unsubscribe)) + utils.CRLF + _unsubscribe + utils.CRLF +
		"$-1" + utils.CRLF +
		":0" + utils.CRLF)
}

func publisMsg(channel string, msg string) []byte {

	return []byte("*3" + utils.CRLF +
		"$" + strconv.Itoa(len(_message)) + utils.CRLF + _message + utils.CRLF +
		"$" + strconv.Itoa(len(channel)) + utils.CRLF + channel + utils.CRLF +
		"$" + strconv.Itoa(len(msg)) + utils.CRLF + msg + utils.CRLF)
}

type Pubhub struct {

	// 自定义实现的map
	dataDict dict.ConcurrentDict

	// 该锁的颗粒度太大
	//locker sync.RWMutex

	locker *locker.Locker // 自定义一个分布锁
}

func NewPubsub() *Pubhub {
	pubsub := &Pubhub{
		dataDict: *dict.NewConcurrentDict(16),
		locker:   locker.NewLocker(16),
	}
	return pubsub
}

// SUBSCRIBE channel [channel ...]
func (p *Pubhub) Subscribe(c abstract.Connection, args [][]byte) protocol.Reply {

	if len(args) < 1 {
		return protocol.NewArgNumErrReply("subscribe")
	}

	// 通道名
	keys := make([]string, 0, len(args))
	for _, arg := range args {
		keys = append(keys, string(arg))
	}
	// 加锁
	p.locker.Locks(keys...)
	defer p.locker.Unlocks(keys...)

	for _, arg := range args {
		chanName := string(arg)
		// 记录当前客户端连接订阅的通道
		c.Subscribe(chanName)

		// 双向链表，记录通道下的客户端连接
		var l *list.LinkedList
		raw, exist := p.dataDict.Get(chanName)
		if !exist { // 说明该channel第一次使用
			l = list.NewLinkedList()
			p.dataDict.Put(chanName, l)
		} else {
			l, _ = raw.(*list.LinkedList)
		}

		// 未订阅
		if !l.Contain(func(actual interface{}) bool {
			return c == actual
		}) {
			// 如果不重复，那就记录订阅
			logger.Debug("subscribe channel [" + chanName + "] success")
			l.Add(c)
		}

		// 回复客户端消息
		_, err := c.Write(channelMsg(_subscribe, chanName, c.SubCount()))
		if err != nil {
			logger.Warn(err)
		}
	}

	return protocol.NewNoReply()
}

// 取消订阅
// unsubscribes itself from all the channels using the UNSUBSCRIBE command without additional arguments
func (p *Pubhub) Unsubscribe(c abstract.Connection, args [][]byte) protocol.Reply {

	var channels []string
	if len(args) < 1 { // 取消全部
		channels = c.GetChannels()
	} else { // 取消指定channel
		channels = make([]string, len(args))
		for i, v := range args {
			channels[i] = string(v)
		}
	}

	p.locker.Locks(channels...)
	defer p.locker.Unlocks(channels...)

	// 说明已经没有订阅的通道
	if len(channels) == 0 {
		c.Write(noChannelMsg())
	}
	for _, channel := range channels {

		// 从客户端中删除当前通道
		c.Unsubscribe(channel)
		// 获取链表
		raw, ok := p.dataDict.Get(channel)
		if ok {
			// 从链表中删除当前客户端
			l, _ := raw.(*list.LinkedList)
			l.DelAllByVal(func(actual interface{}) bool {
				return c == actual
			})

			// 如果链表为空，清理map
			if l.Len() == 0 {
				p.dataDict.Delete(channel)
			}
		}
		c.Write(channelMsg(_unsubscribe, channel, c.SubCount()))
	}

	return protocol.NewNoReply()
}

func (p *Pubhub) Publish(self abstract.Connection, args [][]byte) protocol.Reply {

	if len(args) != 2 {
		return protocol.NewArgNumErrReply("publish")
	}

	channelName := string(args[0])
	// 加锁
	p.locker.Locks(channelName)
	defer p.locker.Unlocks(channelName)

	raw, ok := p.dataDict.Get(channelName)
	if ok {

		var sendSuccess int64
		var failedClient = make(map[interface{}]struct{})
		// 取出链表
		l, _ := raw.(*list.LinkedList)
		// 遍历链表
		l.ForEach(func(i int, val interface{}) bool {

			conn, _ := val.(abstract.Connection)

			if conn.IsClosed() {
				failedClient[val] = struct{}{}
				return true
			}

			if val == self { //不给自己发送
				return true
			}
			// 发送数据
			conn.Write(publisMsg(channelName, string(args[1])))
			sendSuccess++
			return true
		})

		// 剔除客户端
		if len(failedClient) > 0 {
			removed := l.DelAllByVal(func(actual interface{}) bool {
				_, ok := failedClient[actual]
				return ok
			})
			logger.Debugf("del %d closed client", removed)
		}

		// 返回发送的客户端数量
		return protocol.NewIntegerReply(sendSuccess)
	}
	// 如果channel不存在
	return protocol.NewIntegerReply(0)
}
