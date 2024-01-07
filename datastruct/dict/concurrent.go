package dict

import (
	"sync"
	"sync/atomic"

	"github.com/gofish2020/easyredis/utils"
)

// 并发安全的字典
type ConcurrentDict struct {
	shds  []*shard      // 底层shard切片
	mask  uint32        // 掩码
	count *atomic.Int32 // 元素个数
}

type shard struct {
	m  map[string]interface{}
	mu sync.RWMutex
}

func (sh *shard) forEach(consumer Consumer) bool {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	for k, v := range sh.m {
		res := consumer(k, v)
		if !res {
			return false
		}
	}
	return true
}

// 构造字典对象
func NewConcurrentDict(shardCount int) *ConcurrentDict {
	shardCount = utils.ComputeCapacity(shardCount)

	dict := &ConcurrentDict{}
	shds := make([]*shard, shardCount)

	for i := range shds {
		shds[i] = &shard{
			m: make(map[string]interface{}),
		}
	}
	dict.shds = shds
	dict.mask = uint32(shardCount - 1)
	dict.count = &atomic.Int32{}
	return dict
}

// code 对应的索引
func (c *ConcurrentDict) index(code uint32) uint32 {
	return c.mask & code
}

// 获取key对应的shard
func (c *ConcurrentDict) getShard(key string) *shard {
	return c.shds[c.index(utils.Fnv32(key))]
}

// 获取key保存的值
func (c *ConcurrentDict) Get(key string) (val interface{}, exist bool) {
	shd := c.getShard(key)
	shd.mu.RLock()
	defer shd.mu.RUnlock()
	val, exist = shd.m[key]
	return
}

// 元素个数
func (c *ConcurrentDict) Count() int {
	return int(c.count.Load())
}

// 数量+1
func (c *ConcurrentDict) addCount() {
	c.count.Add(1)
}

// 数量-1
func (c *ConcurrentDict) subCount() {
	c.count.Add(-1)
}

// 删除key
func (c *ConcurrentDict) Delete(key string) (interface{}, int) {
	shd := c.getShard(key)
	shd.mu.Lock()
	defer shd.mu.Unlock()

	if val, ok := shd.m[key]; ok {
		delete(shd.m, key)
		c.subCount()
		return val, 1
	}
	return nil, 0
}

// 保存key(insert or update)
func (c *ConcurrentDict) Put(key string, val interface{}) int {

	shd := c.getShard(key)

	shd.mu.Lock()
	defer shd.mu.Unlock()

	if _, ok := shd.m[key]; ok {
		shd.m[key] = val
		return 0 // 更新
	}
	c.addCount()
	shd.m[key] = val
	return 1 // 插入
}

// 保存key( only insert)
func (c *ConcurrentDict) PutIfAbsent(key string, val interface{}) int {

	shd := c.getShard(key)

	shd.mu.Lock()
	defer shd.mu.Unlock()

	if _, ok := shd.m[key]; ok {
		return 0
	}
	c.addCount()
	shd.m[key] = val
	return 1 // 插入
}

// 保存key (only update)
func (c *ConcurrentDict) PutIfExist(key string, val interface{}) int {
	shd := c.getShard(key)

	shd.mu.Lock()
	defer shd.mu.Unlock()
	if _, ok := shd.m[key]; ok {
		shd.m[key] = val
		return 1 // 更新
	}
	return 0
}

// 遍历
func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}
	for _, sh := range dict.shds {
		keepContinue := sh.forEach(consumer)
		if !keepContinue {
			break
		}
	}
}
