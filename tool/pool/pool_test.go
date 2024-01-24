package pool

import (
	"errors"
	"testing"
	"time"
)

type mockConn struct {
	open bool
}

func TestPool(t *testing.T) {
	connNum := 0
	factory := func() (interface{}, error) {
		connNum++
		return &mockConn{
			open: true,
		}, nil
	}
	finalizer := func(x interface{}) {
		connNum--
		c := x.(*mockConn)
		c.open = false
	}
	cfg := Config{
		MaxIdles:  20,
		MaxActive: 40,
	}
	pool := NewPool(factory, finalizer, cfg)
	var borrowed []*mockConn
	for i := 0; i < int(cfg.MaxActive); i++ { // Get
		x, err := pool.Get()
		if err != nil {
			t.Error(err)
			return
		}
		c := x.(*mockConn)
		if !c.open {
			t.Error("conn is not open")
			return
		}
		borrowed = append(borrowed, c)
	}
	for _, c := range borrowed { // Put
		pool.Put(c)
	}

	borrowed = nil
	// borrow returned
	for i := 0; i < int(cfg.MaxActive); i++ { // Get

		x, err := pool.Get()
		if err != nil {
			t.Error(err)
			return
		}
		c := x.(*mockConn)
		if !c.open {
			t.Error("conn is not open")
			return
		}
		borrowed = append(borrowed, c)
	}
	for i, c := range borrowed { // Put
		if i < len(borrowed)-1 {
			pool.Put(c)
		}
	}
	pool.Close()
	pool.Close() // test close twice
	pool.Put(borrowed[len(borrowed)-1])
	if connNum != 0 {
		t.Errorf("%d connections has not closed", connNum)
	}
	_, err := pool.Get()
	if err != ErrClosed {
		t.Error("expect err closed")
	}
}

func TestPool_Waiting(t *testing.T) {
	factory := func() (interface{}, error) {
		return &mockConn{
			open: true,
		}, nil
	}
	finalizer := func(x interface{}) {
		c := x.(*mockConn)
		c.open = false
	}
	cfg := Config{
		MaxIdles:  2,
		MaxActive: 4,
	}
	pool := NewPool(factory, finalizer, cfg)
	var borrowed []*mockConn
	for i := 0; i < int(cfg.MaxActive); i++ { // Get
		x, err := pool.Get()
		if err != nil {
			t.Error(err)
			return
		}
		c := x.(*mockConn)
		if !c.open {
			t.Error("conn is not open")
			return
		}
		borrowed = append(borrowed, c)
	}
	getResult := make(chan bool)
	go func() {
		x, err := pool.Get() // 阻塞
		if err != nil {
			t.Error(err)
			getResult <- false
			return
		}
		c := x.(*mockConn)
		if !c.open {
			t.Error("conn is not open")
			getResult <- false
			return
		}
		getResult <- true
	}()
	time.Sleep(time.Second)
	pool.Put(borrowed[0]) // 放回一个
	if ret := <-getResult; !ret {
		t.Error("get and waiting returned failed")
	}
}

func TestPool_CreateErr(t *testing.T) {
	makeErr := true
	factory := func() (interface{}, error) {
		if makeErr {
			makeErr = false
			return nil, errors.New("mock err")
		}
		return &mockConn{
			open: true,
		}, nil
	}
	finalizer := func(x interface{}) {
		c := x.(*mockConn)
		c.open = false
	}
	cfg := Config{
		MaxIdles:  2,
		MaxActive: 4,
	}
	pool := NewPool(factory, finalizer, cfg)
	_, err := pool.Get() //第一次获取-错误
	if err == nil {
		t.Error("expecting err")
		return
	}
	x, err := pool.Get() // 第二次获取成功
	if err != nil {
		t.Error("get err")
		return
	}
	pool.Put(x)         // 放回
	_, err = pool.Get() // 再获取回来
	if err != nil {
		t.Error("get err")
		return
	}

}
