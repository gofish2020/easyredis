package dict

import (
	"math"
	"sync"
	"sync/atomic"
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

// 计算比param参数大，并满足是2的N次幂, 最近接近param的数值size
func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

// 计算key的hashcode
const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// 构造字典对象
func NewConcurrentDict(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)

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
	return c.shds[c.index(fnv32(key))]
}

// 获取key保存的值
func (c *ConcurrentDict) Get(key string) (val interface{}, exist bool) {
	shd := c.getShard(key)
	shd.mu.Lock()
	defer shd.mu.Unlock()
	val, exist = shd.m[key]
	return
}

// 元素个数
func (c *ConcurrentDict) Count() int {
	return int(c.count.Load())
}

func (c *ConcurrentDict) addCount() {
	c.count.Add(1)
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
