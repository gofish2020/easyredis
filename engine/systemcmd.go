package engine

import (
	"github.com/gofish2020/easyredis/redis/connection"
	"github.com/gofish2020/easyredis/redis/protocal"
	"github.com/gofish2020/easyredis/tool/conf"
)

/*
常用：系统命令
*/
func Ping(redisArgs [][]byte) protocal.Reply {

	if len(redisArgs) == 0 { // 不带参数
		return protocal.NewPONGReply()
	} else if len(redisArgs) == 1 { // 带参数1个
		return protocal.NewBulkReply(redisArgs[0])
	}
	// 否则，回复命令格式错误
	return protocal.NewArgNumErrReply("ping")
}

func checkPasswd(c *connection.KeepConnection) bool {
	// 如果没有配置密码
	if conf.GlobalConfig.RequirePass == "" {
		return true
	}
	// 密码是否一致
	return c.GetPassword() == conf.GlobalConfig.RequirePass
}

func Auth(c *connection.KeepConnection, redisArgs [][]byte) protocal.Reply {
	if len(redisArgs) != 1 {
		return protocal.NewArgNumErrReply("auth")
	}

	if conf.GlobalConfig.RequirePass == "" {
		return protocal.NewGenericErrReply("No authorization is required")
	}

	password := string(redisArgs[0])
	if conf.GlobalConfig.RequirePass != password {
		return protocal.NewGenericErrReply("Auth failed, password is wrong")
	}

	c.SetPassword(password)
	return protocal.NewOkReply()
}
