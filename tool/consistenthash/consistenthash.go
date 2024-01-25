package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
)

type HashFunc func(data []byte) uint32

type Map struct {
	hashFunc  HashFunc       // 计算hash函数
	replicas  int            // 每个节点的虚拟节点数量
	hashValue []int          // hash值
	hashMap   map[int]string // hash值映射的真实节点
}

/*
replicas：副本数量
fn：hash函数
*/
func New(replicas int, fn HashFunc) *Map {
	m := &Map{
		replicas: replicas,
		hashFunc: fn,
		hashMap:  make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

func (m *Map) IsEmpty() bool {
	return len(m.hashValue) == 0
}

// 添加 节点
func (m *Map) Add(ipAddrs ...string) {
	for _, ipAddr := range ipAddrs {
		if ipAddr == "" {
			continue
		}
		// 每个ipAddr 生成 m.replicas个哈希值副本
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hashFunc([]byte(strconv.Itoa(i) + ipAddr)))
			// 记录hash值
			m.hashValue = append(m.hashValue, hash)
			// 映射hash为同一个ipAddr
			m.hashMap[hash] = ipAddr
		}
	}
	sort.Ints(m.hashValue)
}

// support hash tag  example :{key}
func getPartitionKey(key string) string {
	beg := strings.Index(key, "{")
	if beg == -1 {
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == beg+1 {
		return key
	}
	return key[beg+1 : end]
}

// Get gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	partitionKey := getPartitionKey(key)
	hash := int(m.hashFunc([]byte(partitionKey)))

	// 查找 m.keys中第一个大于or等于hash值的元素索引
	idx := sort.Search(len(m.hashValue), func(i int) bool { return m.hashValue[i] >= hash }) //

	// 表示找了一圈没有找到大于or等于hash值的元素，那么默认是第0号元素
	if idx == len(m.hashValue) {
		idx = 0
	}

	// 返回 key应该存储的ipAddr
	return m.hashMap[m.hashValue[idx]]
}
