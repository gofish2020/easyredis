package connection

import (
	"net"
	"sync"
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
}

// 本质就是构建 *KeepConnection对象，存储c net.Conn 以及相关信息
func NewKeepConnection(c net.Conn) *KeepConnection {

	conn, ok := connPool.Get().(*KeepConnection)
	if !ok {
		logger.Error("connection pool make wrong type")
		return &KeepConnection{
			c: c,
		}
	}
	conn.c = c
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

	k.writeDataWaitGroup.WaitWithTimeOut(timeout) // 等待write结束
	k.c.Close()
	k.dbIndex = 0
	connPool.Put(k)
	return nil
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
