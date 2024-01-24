package pool

import (
	"errors"
	"sync"

	"github.com/gofish2020/easyredis/tool/logger"
)

/*
对象池：
1.最多只能缓存 MaxIdles 个空闲对象
2.获取对象的时候，如果没有空闲对象，就创建新对象，对象池中最多只能创建 MaxActive 个对象
*/

var (
	ErrClosed = errors.New("pool closed")
)

type Config struct {
	MaxIdles  int
	MaxActive int
}

type Pool struct {
	Config

	// 创建对象
	newObject func() (any, error)
	// 释放对象
	freeObject func(x any)

	// 空闲对象池
	idles chan any

	mu          sync.Mutex
	activeCount int        // 已经创建的对象个数
	waiting     []chan any // 阻塞等待

	closed bool // 是否已关闭
}

func NewPool(new func() (any, error), free func(x any), conf Config) *Pool {

	if new == nil {
		logger.Error("NewPool argument new func is nil")
		return nil
	}

	if free == nil {
		free = func(x any) {}
	}

	p := Pool{
		Config:      conf,
		newObject:   new,
		freeObject:  free,
		activeCount: 0,
		closed:      false,
	}
	p.idles = make(chan any, p.MaxIdles)
	return &p
}

func (p *Pool) Put(x any) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.freeObject(x) // 直接释放
		return
	}

	//1.先判断等待中
	if len(p.waiting) > 0 {
		// 弹出一个（从头部）
		wait := p.waiting[0]
		temp := make([]chan any, len(p.waiting)-1)
		copy(temp, p.waiting[1:])
		p.waiting = temp
		wait <- x // 取消阻塞
		p.mu.Unlock()
		return

	}
	// 2.直接放回空闲缓冲
	select {
	case p.idles <- x:
		p.mu.Unlock()
	default: // 说明空闲已满
		p.activeCount-- // 对象个数-1
		p.mu.Unlock()
		p.freeObject(x) // 释放
	}

}

func (p *Pool) Get() (any, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrClosed
	}
	select {
	case x := <-p.idles: // 从空闲中获取
		p.mu.Unlock() // 解锁
		return x, nil
	default:
		return p.getOne() // 获取一个新的
	}
}

func (p *Pool) getOne() (any, error) {

	// 说明已经创建了太多对象
	if p.activeCount >= p.Config.MaxActive {

		wait := make(chan any, 1)
		p.waiting = append(p.waiting, wait)
		p.mu.Unlock()
		// 阻塞等待
		x, ok := <-wait
		if !ok {
			return nil, ErrClosed
		}
		return x, nil
	}

	p.activeCount++
	p.mu.Unlock()
	// 创建新对象
	x, err := p.newObject()
	if err != nil {
		p.mu.Lock()
		p.activeCount--
		p.mu.Unlock()
		return nil, err
	}
	return x, nil
}

// 关闭对象池
func (p *Pool) Close() {

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.idles)
	for _, wait := range p.waiting {
		close(wait) // 关闭等待的通道
	}
	p.waiting = nil
	p.mu.Unlock()

	// 释放空闲对象
	for x := range p.idles {
		p.freeObject(x)
	}
}
