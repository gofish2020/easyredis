package engine

import (
	"strings"

	"github.com/gofish2020/easyredis/redis/protocal"
)

/*
命令注册中心：记录命令和命令执行函数之间的映射关系
*/

type ExecFunc func(db *DB, args [][]byte) protocal.Reply

var commandCenter map[string]*command = make(map[string]*command)

type command struct {
	commandName string
	execFunc    ExecFunc
}

func registerCommand(name string, execFunc ExecFunc) {

	name = strings.ToLower(name)
	c := &command{}
	c.commandName = name
	c.execFunc = execFunc
	commandCenter[name] = c
}
