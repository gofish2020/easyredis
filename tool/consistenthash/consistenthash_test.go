package consistenthash

import (
	"testing"

	"github.com/gofish2020/easyredis/utils"
)

func TestConsistentHash(t *testing.T) {

	hashMap := New(100, nil) // replicas越大，分布越均匀

	ipAddrs := []string{"127.0.0.1:7379", "127.0.0.1:8379", "127.0.0.1:6379"}
	hashMap.Add(ipAddrs...)

	constains := make(map[string]int)
	for i := 0; i < 100; i++ {
		value := hashMap.Get(utils.RandString(10))
		constains[value]++
	}

	for k, v := range constains {
		t.Log(k, v)
	}

}
