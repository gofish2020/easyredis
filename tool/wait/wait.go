package wait

import (
	"sync"
	"time"
)

/*
对系统 WaitGroup的封装
*/
type Wait struct {
	wait sync.WaitGroup
}

func (w *Wait) Add(delta int) {
	w.wait.Add(delta)
}

func (w *Wait) Done() {
	w.wait.Done()
}

func (w *Wait) Wait() {
	w.wait.Wait()
}

// 超时等待
func (w *Wait) WaitWithTimeOut(timeout time.Duration) bool {

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		w.Wait()
	}()

	select {
	case <-ch:
		return false // 正常
	case <-time.After(timeout):
		return true // 超时
	}
}
