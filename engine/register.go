package engine

import (
	"strings"

	"github.com/gofish2020/easyredis/redis/protocol"
)

/*
命令注册中心：记录命令和命令执行函数之间的映射关系
*/

type ExecFunc func(db *DB, args [][]byte) protocol.Reply

type KeysFunc func(args [][]byte) ([]string, []string) // read/write

type UndoFunc func(db *DB, args [][]byte) [][][]byte

var commandCenter map[string]*command = make(map[string]*command)

type command struct {
	commandName string
	execFunc    ExecFunc // 命令执行函数
	keyFunc     KeysFunc // 获取命令中的key
	argsNum     int      // redis命令组成个数;例如 get key就是由2部分组成； 如果是负数-2表示要>=2；如果是正数2表示 = 2
	undoFunc    UndoFunc // 生成回滚命令
}

func registerCommand(name string, execFunc ExecFunc, keyFunc KeysFunc, argsNum int, undoFunc UndoFunc) {
	name = strings.ToLower(name)
	cmd := &command{}
	cmd.commandName = name
	cmd.execFunc = execFunc
	cmd.keyFunc = keyFunc
	cmd.argsNum = argsNum
	cmd.undoFunc = undoFunc
	commandCenter[name] = cmd
}

func GetRelatedKeys(redisCommand [][]byte) ([]string, []string) {

	cmdName := strings.ToLower(string(redisCommand[0]))

	cmd, ok := commandCenter[cmdName]
	if !ok {
		return nil, nil
	}

	keyFunc := cmd.keyFunc
	if keyFunc == nil {
		return nil, nil
	}
	return keyFunc(redisCommand[1:])
}
