package engine

import (
	"errors"
	"strings"

	"github.com/gofish2020/easyredis/abstract"
	"github.com/gofish2020/easyredis/redis/protocol"
)

// 事务文档： https://redis.io/docs/interact/transactions/

// 开启事务
func StartMulti(c abstract.Connection) protocol.Reply {
	if c.IsTransaction() {
		return protocol.NewGenericErrReply("multi is already start,do not repeat it")
	}
	// 设定开启
	c.SetTransaction(true)
	return protocol.NewOkReply()
}

// 取消事务
func DiscardMulti(c abstract.Connection) protocol.Reply {
	if !c.IsTransaction() {
		return protocol.NewGenericErrReply("DISCARD without MULTI")
	}
	// 取消开启
	c.SetTransaction(false)
	return protocol.NewOkReply()
}

// 入队：保证命令在格式正确&& 存在的情况下入队
func EnqueueCmd(c abstract.Connection, redisCommand [][]byte) protocol.Reply {

	cmdName := strings.ToLower(string(redisCommand[0]))

	// 从命令注册中心，获取命令的执行函数
	cmd, ok := commandCenter[cmdName]
	if !ok { // 命令不存在
		c.AddTxError(errors.New("unknown command '" + cmdName + "'"))
		return protocol.NewGenericErrReply("unknown command '" + cmdName + "'")
	}

	// 获取key的函数未设置
	if cmd.keyFunc == nil {
		c.AddTxError(errors.New("ERR command '" + cmdName + "' cannot be used in MULTI"))
		return protocol.NewGenericErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
	}

	// 参数个数不对
	if !validateArity(cmd.argsNum, redisCommand) {
		c.AddTxError(errors.New("ERR wrong number of arguments for '" + cmdName + "' command"))
		return protocol.NewArgNumErrReply(cmdName)
	}
	// 入队命令
	c.EnqueueCmd(redisCommand)
	return protocol.NewQueuedReply()
}

// 监视 key [key...]
func Watch(db *DB, conn abstract.Connection, args [][]byte) protocol.Reply {
	if len(args) < 1 {
		return protocol.NewArgNumErrReply("WATCH")
	}
	if conn.IsTransaction() {
		return protocol.NewGenericErrReply("WATCH inside MULTI is not allowed")
	}
	watching := conn.GetWatchKey()
	for _, bkey := range args {
		key := string(bkey)
		watching[key] = db.GetVersion(key) // 保存当前key的版本号（利用版本号机制判断key是否有变化）
	}
	return protocol.NewOkReply()
}

// 清空watch key
func UnWatch(db *DB, conn abstract.Connection) protocol.Reply {
	conn.CleanWatchKey()
	return protocol.NewOkReply()
}

// 执行事务  exec rb
func ExecMulti(db *DB, conn abstract.Connection, args [][]byte) protocol.Reply {

	// 说明当前不是【事务模式】
	if !conn.IsTransaction() {
		return protocol.NewGenericErrReply("EXEC without MULTI")
	}
	// 执行完，自动退出事务模式
	defer conn.SetTransaction(false)

	// 如果在入队的时候，就有格式错误，直接返回
	if len(conn.GetTxErrors()) > 0 {
		return protocol.NewGenericErrReply("EXECABORT Transaction discarded because of previous errors.")
	}

	// 是否自动回滚（这里是自定义的一个参数，标准redis中没有）
	isRollBack := false
	if len(args) > 0 && strings.ToUpper(string(args[0])) == "RB" { // 有rb参数，说明要自动回滚
		isRollBack = true
	}
	// 获取所有的待执行命令
	cmdLines := conn.GetQueuedCmdLine()
	return db.execMulti(conn, cmdLines, isRollBack)
}

// 获取命令的回滚命令
func (db *DB) GetUndoLog(cmdLine [][]byte) []CmdLine {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := commandCenter[cmdName]
	if !ok {
		return nil
	}
	undo := cmd.undoFunc
	if undo == nil {
		return nil
	}
	return undo(db, cmdLine[1:])
}

// 执行事务：本质就是一堆命令一起执行， isRollback 表示出错是否回滚
func (db *DB) execMulti(conn abstract.Connection, cmdLines []CmdLine, isRollback bool) protocol.Reply {

	// 命令的执行结果
	results := make([]protocol.Reply, len(cmdLines))

	versionKeys := make([][]string, len(cmdLines))

	var writeKeys []string
	var readKeys []string
	for idx, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		cmd, ok := commandCenter[cmdName]
		if !ok {
			// 这里正常不会执行
			continue
		}
		keyFunc := cmd.keyFunc
		readKs, writeKs := keyFunc(cmdLine[1:])
		// 读写key
		readKeys = append(readKeys, readKs...)
		writeKeys = append(writeKeys, writeKs...)
		// 写key需要 变更版本号
		versionKeys[idx] = append(versionKeys[idx], writeKs...)
	}

	watchingKey := conn.GetWatchKey()
	if isWatchingChanged(db, watchingKey) { // 判断watch key是否发生了变更
		return protocol.NewEmptyMultiBulkReply()
	}

	// 所有key上锁（原子性）
	db.RWLock(readKeys, writeKeys)
	defer db.RWUnLock(readKeys, writeKeys)

	undoCmdLines := [][]CmdLine{}
	aborted := false
	for idx, cmdLine := range cmdLines {

		// 生成回滚命令
		if isRollback {
			undoCmdLines = append(undoCmdLines, db.GetUndoLog(cmdLine))
		}

		// 执行命令
		reply := db.execWithLock(cmdLine)
		if protocol.IsErrReply(reply) { // 执行出错
			if isRollback { // 需要回滚
				undoCmdLines = undoCmdLines[:len(undoCmdLines)-1] // 命令执行失败（不用回滚），剔除最后一个回滚命令
				aborted = true
				break
			}
		}
		// 执行结果
		results[idx] = reply
	}
	// 中断，执行回滚
	if aborted {
		size := len(undoCmdLines)
		// 倒序执行回滚指令（完成回滚）
		for i := size - 1; i >= 0; i-- {
			curCmdLines := undoCmdLines[i]
			if len(curCmdLines) == 0 {
				continue
			}
			for _, cmdLine := range curCmdLines {
				db.execWithLock(cmdLine)
			}
		}
		return protocol.NewGenericErrReply("EXECABORT Transaction discarded because of previous errors.")
	}

	// 执行到这里，说明命令执行完成（可能全部成功，也可能部分成功）
	for idx, keys := range versionKeys {
		if !protocol.IsErrReply(results[idx]) { // 针对执行成功的命令（写命令），变更版本号
			db.addVersion(keys...)
		}
	}
	// 将多个命令执行的结果，进行合并返回
	mixReply := protocol.NewMixReply()
	mixReply.Append(results...)
	return mixReply
}

func isWatchingChanged(db *DB, watching map[string]int64) bool {
	for key, ver := range watching {
		currentVersion := db.GetVersion(key)
		if ver != currentVersion {
			return true
		}
	}
	return false
}
