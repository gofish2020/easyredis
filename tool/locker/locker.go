package locker

import (
	"sort"
	"sync"

	"github.com/gofish2020/easyredis/utils"
)

type Locker struct {
	mu []*sync.RWMutex

	mask uint32
}

func NewLocker(count int) *Locker {
	l := &Locker{}

	count = utils.ComputeCapacity(count)
	l.mask = uint32(count) - 1
	l.mu = make([]*sync.RWMutex, count)
	for i := 0; i < count; i++ {
		l.mu[i] = &sync.RWMutex{}
	}
	return l
}

// 顺序加锁(互斥)
func (l *Locker) Locks(keys ...string) {
	indexs := l.toLockIndex(keys...)
	for _, index := range indexs {
		mu := l.mu[index]
		mu.Lock()
	}
}

// 顺序解锁(互斥)
func (l *Locker) Unlocks(keys ...string) {
	indexs := l.toLockIndex(keys...)
	for _, index := range indexs {
		mu := l.mu[index]
		mu.Unlock()
	}
}

// 顺序加锁（只读）
func (l *Locker) RLocks(keys ...string) {
	indexs := l.toLockIndex(keys...)
	for _, index := range indexs {
		mu := l.mu[index]
		mu.RLock()
	}
}

// 顺序解锁（只读）
func (l *Locker) RUnlocks(keys ...string) {
	indexs := l.toLockIndex(keys...)
	for _, index := range indexs {
		mu := l.mu[index]
		mu.RUnlock()
	}
}

// 顺序加锁 wkeys和rkeys可以有重叠
func (l *Locker) RWLocks(wkeys []string, rkeys []string) {

	// 所有的key的索引 （内部会去重）
	allKeys := append(wkeys, rkeys...)
	allIndexs := l.toLockIndex(allKeys...)

	// 只写key的索引
	wMapIndex := make(map[uint32]struct{})
	for _, key := range wkeys {
		wMapIndex[l.spread(utils.Fnv32(key))] = struct{}{}
	}

	for _, index := range allIndexs {
		mu := l.mu[index]

		if _, ok := wMapIndex[index]; ok { // 索引是写
			mu.Lock() // 加互斥锁
		} else {
			mu.RLock() // 加只读锁
		}
	}
}

// 顺序解锁 wkeys和rkeys可以有重叠
func (l *Locker) RWUnlocks(wkeys []string, rkeys []string) {
	// 所有的key的索引 （内部会去重）
	allKeys := append(wkeys, rkeys...)
	allIndexs := l.toLockIndex(allKeys...)

	// 只写key的索引
	wMapIndex := make(map[uint32]struct{})
	for _, key := range wkeys {
		wMapIndex[l.spread(utils.Fnv32(key))] = struct{}{}
	}

	for _, index := range allIndexs {
		mu := l.mu[index]

		if _, ok := wMapIndex[index]; ok { // 索引是写
			mu.Unlock() // 解锁
		} else {
			mu.RUnlock() // 解锁
		}
	}
}

func (l *Locker) spread(hashcode uint32) uint32 {
	return hashcode & l.mask
}

func (l *Locker) toLockIndex(keys ...string) []uint32 {

	// 将key转成 切片索引[0,mask]
	mapIndex := make(map[uint32]struct{}) // 去重
	for _, key := range keys {
		mapIndex[l.spread(utils.Fnv32(key))] = struct{}{}
	}

	indices := make([]uint32, 0, len(mapIndex))
	for k := range mapIndex {
		indices = append(indices, k)
	}
	// 对索引排序
	sort.Slice(indices, func(i, j int) bool {
		return indices[i] < indices[j]
	})
	return indices
}
