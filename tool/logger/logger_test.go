package logger

import (
	"testing"
	"time"

	"github.com/gofish2020/easyredis/utils"
)

func TestStd(t *testing.T) {

	Debug("hello redis")
	Debugf("%s", "hello redis format")

	Info("hello redis")
	Infof("%s", "hello redis format")
	Error("hello redis")
	Errorf("%s", "hello redis format")
	Fatal("hello redis")
	Fatalf("%s", "hello redis format")
	Warn("hello redis")
	Warnf("%s", "hello redis format")

	SetLoggerLevel(INFO) // modify log level

	Debug("hello redis")               // don't print
	Debugf("%s", "hello redis format") // don't print
	Info("hello redis")
	Infof("%s", "hello redis format")
	Error("hello redis")
	Errorf("%s", "hello redis format")
	Fatal("hello redis")
	Fatalf("%s", "hello redis format")
	Warn("hello redis")
	Warnf("%s", "hello redis format")
	time.Sleep(3 * time.Second)
}

func TestFile(t *testing.T) {
	Setup(&Settings{
		Path:       "logs",
		Name:       "easyredis",
		Ext:        "log",
		DateFormat: utils.DateFormat,
	})

	SetLoggerLevel(ERROR) // modify log level
	Debug("hello redis")
	Debugf("%s", "hello redis format")
	Info("hello redis")
	Infof("%s", "hello redis format")
	Error("hello redis")
	Errorf("%s", "hello redis format")
	Fatal("hello redis")
	Fatalf("%s", "hello redis format")
	Warn("hello redis")
	Warnf("%s", "hello redis format")
	<-time.After(3 * time.Second)
}
