package aof

import (
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofish2020/easyredis/abstract"
	"github.com/gofish2020/easyredis/redis/connection"
	"github.com/gofish2020/easyredis/redis/parser"
	"github.com/gofish2020/easyredis/redis/protocol"
	"github.com/gofish2020/easyredis/tool/logger"
)

/*
原理类似于写日志，engine 中只会持有一个aof对象，通过生产者消费者模型，将数据写入到磁盘文件中
*/

const (
	aofChanSize = 1 << 20
)

const (
	// 每次写入命令 & 刷盘
	FsyncAlways = "always"
	// 每秒刷盘
	FsyncEverySec = "everysec"
	// 不主动刷盘，取决于操作系统刷盘
	FsyncNo = "no"
)

type Command = [][]byte

type aofRecord struct {
	dbIndex int
	command Command
}

type AOF struct {
	// aof 文件句柄
	aofFile *os.File
	// aof 文件路径
	aofFileName string
	// 刷盘间隔
	aofFsync string
	// 最后写入aof日志的数据库索引
	lastDBIndex int
	// 保存aof记录通道
	aofChan chan aofRecord
	// 互斥锁
	mu sync.Mutex

	// aofChan读取完毕
	aofFinished chan struct{}
	// 关闭定时刷盘
	closed chan struct{}
	// 禁止aofChan的写入
	atomicClose atomic.Bool
	// 引擎 *Engine
	engine abstract.Engine
}

// 构建AOF对象
func NewAOF(aofFileName string, engine abstract.Engine, load bool, fsync string) (*AOF, error) {
	aof := &AOF{}
	aof.aofFileName = aofFileName
	aof.aofFsync = strings.ToLower(fsync)
	aof.lastDBIndex = 0
	aof.aofChan = make(chan aofRecord, aofChanSize)
	aof.closed = make(chan struct{})
	aof.aofFinished = make(chan struct{})
	aof.engine = engine
	aof.atomicClose.Store(false)

	// 启动加载aof文件
	if load {
		aof.LoadAof(0)
	}

	// 打开文件(追加写/创建/读写)
	aofFile, err := os.OpenFile(aof.aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	aof.aofFile = aofFile

	// 启动协程：每秒刷盘
	if aof.aofFsync == FsyncEverySec {
		aof.fsyncEverySec()
	}
	// 启动协程：检测aofChan
	go aof.watchChan()
	return aof, nil
}

func (aof *AOF) watchChan() {

	for record := range aof.aofChan {
		aof.writeAofRecord(record)
	}
	aof.aofFinished <- struct{}{}
}

func (aof *AOF) SaveRedisCommand(dbIndex int, command Command) {

	// 关闭
	if aof.atomicClose.Load() {
		return
	}
	// 写入文件 & 刷盘
	if aof.aofFsync == FsyncAlways {
		record := aofRecord{
			dbIndex: dbIndex,
			command: command,
		}
		aof.writeAofRecord(record)
		return
	}
	// 写入缓冲
	aof.aofChan <- aofRecord{
		dbIndex: dbIndex,
		command: command,
	}
}

func (aof *AOF) writeAofRecord(record aofRecord) {

	aof.mu.Lock()
	defer aof.mu.Unlock()

	// 因为aof对象是所有数据库对象【复用】写入文件方法，每个数据库的索引不同
	// 所以，每个命令的执行，有个前提就是操作的不同的数据库
	if record.dbIndex != aof.lastDBIndex {
		// 构建select index 命令 & 写入文件
		selectCommand := [][]byte{[]byte("select"), []byte(strconv.Itoa(record.dbIndex))}
		data := protocol.NewMultiBulkReply(selectCommand).ToBytes()
		_, err := aof.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
			return
		}
		aof.lastDBIndex = record.dbIndex
	}

	// redis命令
	data := protocol.NewMultiBulkReply(record.command).ToBytes()
	_, err := aof.aofFile.Write(data)
	if err != nil {
		logger.Warn(err)
	}
	logger.Debugf("write aof command:%q", data)
	// 每次写入刷盘
	if aof.aofFsync == FsyncAlways {
		aof.aofFile.Sync()
	}
}

func (aof *AOF) Fsync() {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	if err := aof.aofFile.Sync(); err != nil {
		logger.Errorf("aof sync err:%+v", err)
	}
}

func (aof *AOF) fsyncEverySec() {
	// 每秒刷盘
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				aof.Fsync()
			case <-aof.closed:
				return
			}
		}
	}()
}

// 加载aof文件,maxBytes限定读取的字节数
func (aof *AOF) LoadAof(maxBytes int) {

	// 目的：当加载aof文件的时候，因为需要复用engine对象，内部重放命令的时候会自动写aof日志，加载aof 禁用 SaveRedisCommand的写入
	aof.atomicClose.Store(true)
	defer func() {
		aof.atomicClose.Store(false)
	}()

	// 只读打开文件
	file, err := os.Open(aof.aofFileName)
	if err != nil {
		logger.Error(err.Error())
		return
	}
	defer file.Close()
	file.Seek(0, io.SeekStart)

	var reader io.Reader
	if maxBytes > 0 { // 限定读取的字节大小
		reader = io.LimitReader(file, int64(maxBytes))
	} else { // 不限定，直接读取到文件结尾（为止）
		reader = file
	}

	// 文件中保存的格式和网络传输的格式一致
	ch := parser.ParseStream(reader)
	virtualConn := connection.NewVirtualConn()

	for payload := range ch {
		if payload.Err != nil {
			// 文件已经读取到“完成“
			if payload.Err == io.EOF {
				break
			}
			// 读取到非法的格式
			logger.Errorf("LoadAof parser error %+v:", payload.Err)
			continue
		}

		if payload.Reply == nil {
			logger.Error("empty payload data")
			continue
		}
		// 从文件中读取到命令
		reply, ok := payload.Reply.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}

		// 利用数据库引擎，将命令数据保存到内存中（命令重放）
		ret := aof.engine.Exec(virtualConn, reply.RedisCommand)
		// 判断是否执行失败
		if protocol.IsErrReply(ret) {
			logger.Error("exec err ", string(ret.ToBytes()))
		}
		// 判断命令是否是"select"
		if strings.ToLower(string(reply.RedisCommand[0])) == "select" {
			dbIndex, err := strconv.Atoi(string(reply.RedisCommand[1]))
			if err == nil {
				aof.lastDBIndex = dbIndex // 记录下数据恢复过程中，选中的数据库索引
			}
		}
	}
}

func (aof *AOF) Close() {

	if aof.aofFile != nil {
		// 禁止写入
		aof.atomicClose.CompareAndSwap(false, true)
		// 停止每秒刷盘
		close(aof.closed)
		// aofChan关闭，chan缓冲中可能还有数据
		close(aof.aofChan)
		// 等待缓冲中处理完成 aofFile句柄才会不被使用
		<-aof.aofFinished
		// 关闭文件句柄
		aof.aofFile.Close()
	}
}
