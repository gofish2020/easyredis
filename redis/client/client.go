package client

import (
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofish2020/easyredis/redis/parser"
	"github.com/gofish2020/easyredis/redis/protocol"
	"github.com/gofish2020/easyredis/tool/logger"
	"github.com/gofish2020/easyredis/tool/wait"
)

const (
	maxChanSize = 1 << 10
	maxWait     = 3 * time.Second

	heartBeatInterval = 1 * time.Second
)

const (
	connCreated = iota
	connRunning
	connClosed
)

type request struct {
	command [][]byte       // redis命令
	err     error          // 处理出错
	reply   protocol.Reply // 处理结果

	wait wait.Wait // 等待处理完成
}

func (r *request) Bytes() []byte {
	return protocol.NewMultiBulkReply(r.command).ToBytes()
}

type RedisClient struct {
	// socket连接
	conn net.Conn

	addr string
	// 客户端当前状态
	connStatus atomic.Int32

	// heartbeat
	ticker time.Ticker

	// buffer cache
	waitSend   chan *request
	waitResult chan *request

	// 有请求正在处理中...
	working sync.WaitGroup
}

// 创建redis客户端socket
func NewRedisClient(addr string) (*RedisClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	rc := RedisClient{}
	rc.conn = conn
	rc.waitSend = make(chan *request, maxChanSize)
	rc.waitResult = make(chan *request, maxChanSize)
	rc.addr = addr
	rc.connStatus.Store(connCreated)
	return &rc, nil
}

// 启动
func (rc *RedisClient) Start() error {
	rc.ticker = *time.NewTicker(heartBeatInterval)
	// 将waitSend缓冲区进行发送
	go rc.execSend()
	// 获取服务端结果
	go rc.execReceive()
	// 定时发送心跳
	//go rc.execHeardBeat()
	rc.connStatus.Store(connRunning) // 启动状态
	return nil
}

func (rc *RedisClient) execReceive() {

	ch := parser.ParseStream(rc.conn)

	for payload := range ch {

		if payload.Err != nil {
			if rc.connStatus.Load() == connClosed { // 连接已关闭
				return
			}

			// 否则，重新连接（可能因为网络抖动临时断开了）

			rc.reconnect()
			return
		}

		// 说明一切正常

		rc.handleResult(payload.Reply)
	}
}

func (rc *RedisClient) reconnect() {
	logger.Info("redis client reconnect...")
	rc.conn.Close()

	var conn net.Conn
	// 重连（重试3次）
	for i := 0; i < 3; i++ {
		var err error
		conn, err = net.Dial("tcp", rc.addr)
		if err != nil {
			logger.Error("reconnect error: " + err.Error())
			time.Sleep(time.Second)
			continue
		} else {
			break
		}
	}
	// 服务端连不上，说明服务可能挂了（or 网络问题 and so on...)
	if conn == nil {
		rc.Stop()
		return
	}

	// 这里关闭没问题，因为rc.conn.Close已经关闭，函数Send中保存的请求因为发送不成功，不会写入到waitResult
	close(rc.waitResult)
	// 清理 waitResult(因为连接重置，新连接上只能处理新请求，老的请求的数据结果在老连接上,老连接已经关了，新连接上肯定是没有结果的)
	for req := range rc.waitResult {
		req.err = errors.New("connect reset")
		req.wait.Done()
	}

	// 新连接（新气象）
	rc.waitResult = make(chan *request, maxWait)
	rc.conn = conn

	// 重新启动接收协程
	go rc.execReceive()
}

func (rc *RedisClient) handleResult(reply protocol.Reply) {
	// 从rc.waitResult 获取一个等待中的请求，将结果保存进去
	req := <-rc.waitResult
	if req == nil {
		return
	}
	req.reply = reply
	req.wait.Done() // 通知已经获取到结果
}

// 将waitSend缓冲区进行发送
func (rc *RedisClient) execSend() {
	for req := range rc.waitSend {
		rc.sendReq(req)
	}
}

func (rc *RedisClient) sendReq(req *request) {
	// 无效请求
	if req == nil || len(req.command) == 0 {
		return
	}

	var err error
	// 网络请求（重试3次）
	for i := 0; i < 3; i++ {
		_, err = rc.conn.Write(req.Bytes())
		// 发送成功 or 发送错误（除了超时错误和deadline错误）跳出
		if err == nil ||
			(!strings.Contains(err.Error(), "timeout") && // only retry timeout
				!strings.Contains(err.Error(), "deadline exceeded")) {
			break
		}
	}

	if err == nil { // 发送成功，异步等待结果
		rc.waitResult <- req
	} else { // 发送失败，请求直接失败
		req.err = err
		req.wait.Done()
	}
}

// 定时发送心跳
func (rc *RedisClient) execHeardBeat() {
	for range rc.ticker.C {
		rc.Send([][]byte{[]byte("PING")})
	}

}

// 将redis命令保存到 waitSend 中
func (rc *RedisClient) Send(command [][]byte) (protocol.Reply, error) {

	// 已关闭
	if rc.connStatus.Load() == connClosed {
		return nil, errors.New("client closed")
	}

	req := &request{
		command: command,
		wait:    wait.Wait{},
	}
	// 单个请求
	req.wait.Add(1)

	// 所有请求
	rc.working.Add(1)
	defer rc.working.Done()

	// 将数据保存到缓冲中
	rc.waitSend <- req

	// 等待处理结束
	if req.wait.WaitWithTimeOut(maxWait) {
		return nil, errors.New("time out")
	}
	// 出错
	if req.err != nil {
		err := req.err
		return nil, err
	}
	// 正常
	return req.reply, nil
}

func (rc *RedisClient) Stop() {
	// 设置已关闭
	rc.connStatus.Store(connClosed)
	rc.ticker.Stop()

	// 保证发送协程停止
	close(rc.waitSend)
	// 说明等待网络请求结果的request客户端不阻塞了（也就是剩下的req不需要等待了，可以关闭网络连接）
	rc.working.Wait()
	rc.conn.Close()
	close(rc.waitResult)
}
