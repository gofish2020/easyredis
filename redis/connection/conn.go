package connection

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofish2020/easyredis/tool/logger"
	"github.com/gofish2020/easyredis/tool/wait"
)

const (
	timeout = 10 * time.Second
)

// 连接池对象
var connPool = sync.Pool{
	New: func() interface{} {
		return &KeepConnection{
			dbIndex:  0,
			c:        nil,
			password: "",
			closed:   atomic.Bool{},
			trx:      atomic.Bool{},
		}
	},
}

// 记录连接的相关信息
type KeepConnection struct {
	// 网络conn
	c net.Conn
	// 服务密码
	password string
	// 当前连接指定的数据库
	dbIndex int

	// 当要关闭连接，如果连接还在使用中【等待...】 wait.Wait 是对 sync.WaitGroup的封装
	writeDataWaitGroup wait.Wait

	// 记录当前连接，订阅的channel

	mu   sync.Mutex
	subs map[string]struct{}

	closed atomic.Bool

	// 事务模式
	trx      atomic.Bool
	queue    [][][]byte
	watchKey map[string]int64
	txErrors []error
}

// 本质就是构建 *KeepConnection对象，存储c net.Conn 以及相关信息
func NewKeepConnection(c net.Conn) *KeepConnection {

	conn, ok := connPool.Get().(*KeepConnection)
	if !ok {
		logger.Error("connection pool make wrong type")
		return &KeepConnection{
			dbIndex:  0,
			c:        nil,
			password: "",
			closed:   atomic.Bool{},
			trx:      atomic.Bool{},
		}
	}
	conn.c = c
	conn.closed.Store(false)
	conn.trx.Store(false)
	conn.queue = nil
	conn.txErrors = nil
	conn.watchKey = nil
	return conn
}

// 当前连接选定的数据库
func (k *KeepConnection) SetDBIndex(index int) {
	k.dbIndex = index
}

func (k *KeepConnection) GetDBIndex() int {
	return k.dbIndex
}

// 连接远程地址信息
func (k *KeepConnection) RemoteAddr() string {

	return k.c.RemoteAddr().String()
}

// 关闭 *KeepConnection 对象
func (k *KeepConnection) Close() error {

	k.closed.Store(true)
	k.writeDataWaitGroup.WaitWithTimeOut(timeout) // 等待write结束
	k.c.Close()
	k.dbIndex = 0
	connPool.Put(k)
	return nil
}

func (k *KeepConnection) IsClosed() bool {
	return k.closed.Load()
}

func (k *KeepConnection) Write(b []byte) (int, error) {

	if len(b) == 0 {
		return 0, nil
	}

	k.writeDataWaitGroup.Add(1)       // 说明在write
	defer k.writeDataWaitGroup.Done() // 说明write结束
	return k.c.Write(b)
}

// 密码信息
func (k *KeepConnection) SetPassword(password string) {
	k.password = password
}

func (k *KeepConnection) GetPassword() string {
	return k.password
}

func (k *KeepConnection) Subscribe(channel string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.subs == nil {
		k.subs = map[string]struct{}{}
	}

	k.subs[channel] = struct{}{}

}

func (k *KeepConnection) Unsubscribe(channel string) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if len(k.subs) == 0 {
		return
	}

	delete(k.subs, channel)
}

func (k *KeepConnection) SubCount() int {
	return len(k.subs)
}

func (k *KeepConnection) GetChannels() []string {

	k.mu.Lock()
	defer k.mu.Unlock()

	var result []string
	for channel := range k.subs {
		result = append(result, channel)
	}
	return result
}

func (k *KeepConnection) IsTransaction() bool {
	return k.trx.Load()
}

func (k *KeepConnection) SetTransaction(val bool) {
	if !val { // 取消事务模式，清空队列和watch key
		k.queue = nil
		k.watchKey = nil
		k.txErrors = nil
	}
	// 开启事务状态
	k.trx.Store(val)
}

func (k *KeepConnection) EnqueueCmd(redisCommand [][]byte) {
	k.queue = append(k.queue, redisCommand)
}

func (k *KeepConnection) GetQueuedCmdLine() [][][]byte {
	return k.queue
}
func (k *KeepConnection) GetWatchKey() map[string]int64 {
	if k.watchKey == nil {
		k.watchKey = make(map[string]int64)
	}
	return k.watchKey
}

func (k *KeepConnection) CleanWatchKey() {
	k.watchKey = nil
}

func (k *KeepConnection) GetTxErrors() []error {
	return k.txErrors
}

func (k *KeepConnection) AddTxError(err error) {
	k.txErrors = append(k.txErrors, err)
}
