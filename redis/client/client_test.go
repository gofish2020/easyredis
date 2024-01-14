package client

import (
	"bytes"
	"testing"
	"time"

	"github.com/gofish2020/easyredis/tool/logger"
)

func TestReconnect(t *testing.T) {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "easyredis",
		Ext:        ".log",
		DateFormat: "2006-01-02",
	})
	client, err := NewRedisClient("localhost:6379")
	if err != nil {
		t.Error(err)
	}
	client.Start()

	// 模拟连接断开
	_ = client.conn.Close()
	time.Sleep(time.Second) // wait for reconnecting
	success := false
	for i := 0; i < 3; i++ {
		result, err := client.Send([][]byte{
			[]byte("PING"),
		})
		if err == nil && bytes.Equal(result.ToBytes(), []byte("+PONG\r\n")) {
			success = true
			break
		}
	}
	if !success {
		t.Error("reconnect error")
	}
}
