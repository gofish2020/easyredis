package locker

import (
	"testing"
	"time"

	"github.com/gofish2020/easyredis/tool/logger"
)

var locker *Locker

func TestMain(m *testing.M) {

	locker = NewLocker(8)
	m.Run()
}

func Test2LockIndex(t *testing.T) {

	keys := []string{"1", "2", "3"}
	logger.Debug(locker.toLockIndex(keys...))

	locker.Locks(keys...)
	locker.Unlocks(keys...)

	locker.RLocks(keys...)
	locker.RUnlocks(keys...)
	time.Sleep(3 * time.Second)
}

func TestRWLocker(t *testing.T) {

	wKeys := []string{"1", "2", "3"}
	logger.Debug(locker.toLockIndex(wKeys...))
	rKeys := []string{"3", "4", "5"}
	logger.Debug(locker.toLockIndex(rKeys...))

	locker.RWLocks(wKeys, rKeys)
	locker.RWUnlocks(wKeys, rKeys)

	time.Sleep(1 * time.Second)
}
