package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/gofish2020/easyredis/redis"
	"github.com/gofish2020/easyredis/tcpserver"
	"github.com/gofish2020/easyredis/tool/conf"
	"github.com/gofish2020/easyredis/tool/logger"
	"github.com/gofish2020/easyredis/utils"
)

func main() {
	//1. 打印logo
	println(utils.Logo())

	//2. 初始化配置
	initConfig()

	//3. 日志库初始化
	initLogger()

	logger.Info("start easyredis server")

	//4. 服务对象
	tcp := tcpserver.NewTCPServer(tcpserver.TCPConfig{
		Addr: fmt.Sprintf("%s:%d", conf.GlobalConfig.Bind, conf.GlobalConfig.Port),
	}, redis.NewRedisHandler())

	//5. 启动服务
	err := tcp.Start()
	if err != nil {
		log.Printf("%+v", err)
		os.Exit(1)
	}

	//6. 关闭服务
	tcp.Close()
}

func initLogger() {
	logger.Setup(&logger.Settings{
		Path:       conf.GlobalConfig.Dir + "/logs",
		Name:       "easyredis",
		Ext:        "log",
		DateFormat: utils.DateFormat,
	})

	logger.SetLoggerLevel(logger.DEBUG)
}

func initConfig() {
	configFileName := ""
	flag.StringVar(&configFileName, "conf", "", "Usage: -conf=./redis.conf")
	flag.Parse()

	//  解析配置文件
	if configFileName == "" {
		configFileName = utils.ExecDir() + "/redis.conf"
	}
	if utils.FileExists(configFileName) {
		conf.LoadConfig(configFileName)
	} else {
		// 默认的配置
		conf.GlobalConfig = &conf.RedisConfig{
			Bind:           "0.0.0.0",
			Port:           6379,
			AppendOnly:     false,
			AppendFilename: "",
			RunID:          utils.RandString(40),
		}
	}
	//logger.Debugf("%#v", conf.GlobalConfig)
}
