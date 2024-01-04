package aof

import (
	"io"
	"os"
	"strconv"
	"time"

	"github.com/gofish2020/easyredis/abstract"
	"github.com/gofish2020/easyredis/engine/payload"
	"github.com/gofish2020/easyredis/redis/protocol"
	"github.com/gofish2020/easyredis/tool/conf"
	"github.com/gofish2020/easyredis/tool/logger"
)

type snapshotAOF struct {
	fileSize int64 // 文件大小
	dbIndex  int   // 数据库索引
	tempFile *os.File
}

func (aof *AOF) Rewrite(engine abstract.Engine) {
	//1.对现有的aof文件做一次快照
	snapShot, err := aof.startRewrite()
	if err != nil {
		logger.Errorf("StartRewrite err: %+v", err)
		return
	}

	//2. 将现在的aof文件数据，加在到新（内存）对象中,并重写入新aof文件中
	err = aof.doRewrite(snapShot, engine)
	if err != nil {
		logger.Errorf("doRewrite err: %+v", err)
		return
	}

	//3. 将重写过程中的增量命令写入到新文件中
	err = aof.finishRewrite(snapShot)
	if err != nil {
		logger.Errorf("finishRewrite err: %+v", err)
	}
}

func (aof *AOF) startRewrite() (*snapshotAOF, error) {

	// 加锁
	aof.mu.Lock()
	defer aof.mu.Unlock()

	// 文件刷盘
	err := aof.aofFile.Sync()
	if err != nil {
		return nil, err
	}

	// 获取当前文件信息
	fileInfo, err := aof.aofFile.Stat()
	if err != nil {
		return nil, err
	}

	// 创建新的aof文件
	file, err := os.CreateTemp(conf.TmpDir(), "*.aof")
	if err != nil {
		return nil, err
	}

	// 当前aof重写前的信息
	snapShot := &snapshotAOF{}
	// 大小
	snapShot.fileSize = fileInfo.Size()
	// 选中的数据库
	snapShot.dbIndex = aof.lastDBIndex
	snapShot.tempFile = file
	return snapShot, nil
}

func (aof *AOF) doRewrite(snapShot *snapshotAOF, engine abstract.Engine) error {
	// 临时aof对象
	tmpAof := &AOF{}
	tmpAof.aofFileName = aof.aofFileName
	tmpAof.engine = engine
	// 临时aof，加载aof文件，并将数据保存到临时内存中（engine）
	tmpAof.LoadAof(int(snapShot.fileSize))
	// 扫描临时内存，将结果保存到新的aof文件中
	tmpFile := snapShot.tempFile
	for i := 0; i < conf.GlobalConfig.Databases; i++ {
		// 写入 select index
		data := protocol.NewMultiBulkReply(SelectCmd([]byte(strconv.Itoa(i))))
		_, err := tmpFile.Write(data.ToBytes())
		if err != nil {
			return err
		}

		// 遍历数据库
		tmpAof.engine.ForEach(i, func(key string, data *payload.DataEntity, expiration *time.Time) bool {
			// 写入 redis命令
			cmd := EntityToCmd(key, data)
			tmpFile.Write(cmd.ToBytes())
			// 写入过期时间（如果存在的话）
			if expiration != nil {
				cmd := protocol.NewMultiBulkReply(PExpireAtCmd(key, *expiration))
				tmpFile.Write(cmd.ToBytes())
			}
			return true
		})
	}
	return nil

}

func (aof *AOF) finishRewrite(snapshot *snapshotAOF) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	err := aof.aofFile.Sync()
	if err != nil {
		return err
	}

	tmpFile := snapshot.tempFile

	lastCopy := func() error {
		// 打开现有aof
		src, err := os.Open(aof.aofFileName)
		if err != nil {
			return err
		}

		defer func() {
			src.Close()
			tmpFile.Close()
		}()
		// 将游标移动到上次的快照位置
		_, err = src.Seek(snapshot.fileSize, io.SeekStart)
		if err != nil {
			return err
		}

		// 将快照的dbindex保存到tmpFile中（因为增量的命令是在当时快照的时候数据库下生成的）
		data := protocol.NewMultiBulkReply(SelectCmd([]byte(strconv.Itoa(snapshot.dbIndex))))
		_, err = tmpFile.Write(data.ToBytes())
		if err != nil {
			return err
		}

		// 将增量的数据保存到tmpFile中
		_, err = io.Copy(tmpFile, src)
		if err != nil {
			return err
		}
		return nil
	}

	if err := lastCopy(); err != nil {
		return err
	}
	// 执行到这里说明数据复制完毕

	// 关闭当前的aof
	aof.aofFile.Close()

	// 将临时文件移动到aof文件
	if err := os.Rename(tmpFile.Name(), aof.aofFileName); err != nil {
		logger.Warn(err)
	}

	// 重新打开
	aofFile, err := os.OpenFile(aof.aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	aof.aofFile = aofFile

	// 将当前的数据库索引写入
	data := protocol.NewMultiBulkReply(SelectCmd([]byte(strconv.Itoa(aof.lastDBIndex))))
	_, err = aof.aofFile.Write(data.ToBytes())
	if err != nil {
		panic(err)
	}

	return nil
}
