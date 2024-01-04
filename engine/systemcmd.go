package engine

import (
	"github.com/gofish2020/easyredis/abstract"
	"github.com/gofish2020/easyredis/redis/protocol"
	"github.com/gofish2020/easyredis/tool/conf"
)

/*
常用：系统命令
*/
func Ping(redisArgs [][]byte) protocol.Reply {

	if len(redisArgs) == 0 { // 不带参数
		return protocol.NewPONGReply()
	} else if len(redisArgs) == 1 { // 带参数1个
		return protocol.NewBulkReply(redisArgs[0])
	}
	// 否则，回复命令格式错误
	return protocol.NewArgNumErrReply("ping")
}

func checkPasswd(c abstract.Connection) bool {
	// 如果没有配置密码
	if conf.GlobalConfig.RequirePass == "" {
		return true
	}
	// 密码是否一致
	return c.GetPassword() == conf.GlobalConfig.RequirePass
}

func Auth(c abstract.Connection, redisArgs [][]byte) protocol.Reply {
	if len(redisArgs) != 1 {
		return protocol.NewArgNumErrReply("auth")
	}

	if conf.GlobalConfig.RequirePass == "" {
		return protocol.NewGenericErrReply("No authorization is required")
	}

	password := string(redisArgs[0])
	if conf.GlobalConfig.RequirePass != password {
		return protocol.NewGenericErrReply("Auth failed, password is wrong")
	}

	c.SetPassword(password)
	return protocol.NewOkReply()
}
