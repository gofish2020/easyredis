package timewheel

import "time"

type Delay struct {
	tw *TimeWheel
}

func NewDelay() *Delay {
	delay := &Delay{}
	delay.tw = New(1*time.Second, 3600)
	delay.tw.Start()
	return delay
}

// 添加延迟任务 绝对时间
func (d *Delay) AddAt(expire time.Time, key string, callback func()) {
	interval := time.Until(expire)
	d.Add(interval, key, callback)
}

// 添加延迟任务 相对时间
func (d *Delay) Add(interval time.Duration, key string, callback func()) {
	d.tw.Add(interval, key, callback)
}

// 取消延迟任务
func (d *Delay) Cancel(key string) {
	d.tw.Cancel(key)
}
