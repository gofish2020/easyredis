package timewheel

import (
	"testing"
	"time"

	"github.com/gofish2020/easyredis/tool/logger"
)

func TestAdd(t *testing.T) {
	ch := make(chan time.Time)
	beginTime := time.Now()
	delay := NewDelay()
	delay.Add(time.Second, "", func() {
		logger.Debug("exec task...")
		ch <- time.Now()
	})
	execAt := <-ch
	delayDuration := execAt.Sub(beginTime)
	// usually 1.0~2.0 s
	if delayDuration < time.Second || delayDuration > 3*time.Second {
		t.Error("wrong execute time")
	}
}

func TestAddTask(t *testing.T) {
	delay := NewDelay()
	delay.Add(0*time.Second, "test0", func() {
		logger.Info("0 time.Second running")
		time.Sleep(10 * time.Second)
	})

	time.Sleep(1500 * time.Millisecond)

	delay.Add(9*time.Second, "testKey", func() {
		logger.Info("9 time.Second running")
		time.Sleep(5 * time.Second)
	})

	time.Sleep(14 * time.Second)
}

func TestCancelTask(t *testing.T) {
	delay := NewDelay()
	delay.Add(0*time.Second, "test0", func() {
		logger.Info("0 time.Second running")
		time.Sleep(10 * time.Second)
	})

	time.Sleep(1500 * time.Millisecond)

	delay.Add(9*time.Second, "testKey", func() {
		logger.Info("9 time.Second running")
		time.Sleep(5 * time.Second)
	})

	delay.Cancel("testKey")
	time.Sleep(14 * time.Second)
}
