package timewheel

import "time"

var tw *TimeWheel = New(1*time.Second, 3600)

func init() {
	tw.Start()
}

// 添加延迟任务 绝对时间
func AddAt(expire time.Time, key string, callback func()) {
	interval := time.Until(expire)
	Add(interval, key, callback)
}

// 添加延迟任务 相对时间
func Add(interval time.Duration, key string, callback func()) {
	tw.Add(interval, key, callback)
}

// 取消延迟任务
func Cancel(key string) {
	tw.Cancel(key)
}
