package tcpserver

import (
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gofish2020/easyredis/redis"
	"github.com/gofish2020/easyredis/tool/logger"
)

type TCPConfig struct {
	Addr string
}

type TCPServer struct {
	listener      net.Listener   // 监听句柄
	waitDone      sync.WaitGroup // 优雅关闭（等待）
	clientCounter int64          // 有多少个客户端在执行中
	conf          TCPConfig      // 配置
	closeTcp      int32          // 关闭标识
	quit          chan os.Signal // 监听进程信号
	redisHander   redis.Handler  // 实际处理连接对象
}

func NewTCPServer(conf TCPConfig, handler redis.Handler) *TCPServer {
	server := &TCPServer{
		conf:          conf,
		closeTcp:      0,
		clientCounter: 0,
		quit:          make(chan os.Signal, 1),
		redisHander:   handler,
	}
	return server
}

func (t *TCPServer) Start() error {
	// 开启监听
	listen, err := net.Listen("tcp", t.conf.Addr)
	if err != nil {
		return err
	}
	t.listener = listen
	logger.Infof("bind %s listening...", t.conf.Addr)
	// 接收连接
	go t.accept()
	// 阻塞于信号
	signal.Notify(t.quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	<-t.quit
	return nil
}

// accept 死循环接收新连接的到来
func (t *TCPServer) accept() error {

	for {
		conn, err := t.listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				logger.Infof("accept occurs temporary error: %v, retry in 5ms", err)
				time.Sleep(5 * time.Millisecond)
				continue
			}
			// 说明监听listener关闭，无法接收新连接
			logger.Warn(err.Error())
			atomic.CompareAndSwapInt32(&t.closeTcp, 0, 1)
			// 整个进程退出
			t.quit <- syscall.SIGTERM
			// 结束 for循环
			break
		}
		// 启动一个协程处理conn
		go t.handleConn(conn)
	}

	return nil
}

func (t *TCPServer) handleConn(conn net.Conn) {
	// 如果已关闭，新连接不再处理
	if atomic.LoadInt32(&t.closeTcp) == 1 {
		// 直接关闭
		conn.Close()
		return
	}

	logger.Debugf("accept new conn %s", conn.RemoteAddr().String())
	t.waitDone.Add(1)
	atomic.AddInt64(&t.clientCounter, 1)
	defer func() {
		t.waitDone.Done()
		atomic.AddInt64(&t.clientCounter, -1)
	}()

	// TODO :处理连接
	t.redisHander.Handle(context.Background(), conn)
}

// 退出前,清理
func (t *TCPServer) Close() {
	logger.Info("graceful shutdown easyredis server")

	atomic.CompareAndSwapInt32(&t.closeTcp, 0, 1)
	// 关闭监听
	t.listener.Close()
	// 关闭处理对象
	t.redisHander.Close()
	// 阻塞中...(优雅关闭)
	t.waitDone.Wait()
}
