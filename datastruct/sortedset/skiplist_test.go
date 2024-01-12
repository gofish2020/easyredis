package sortedset

import "testing"

func TestRandomLevel(t *testing.T) {
	m := make(map[int16]int)
	for i := 0; i < 10000; i++ {
		level := randomLevel() // [1,16]
		m[level]++
	}

	// 每个层级之间大约1/2的比例
	for i := 0; i <= defaultMaxLevel; i++ {
		t.Logf("level %d, count %d", i, m[int16(i)])
	}
}
