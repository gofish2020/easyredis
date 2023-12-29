package payload

// 定义底层的数据存储对象
type DataEntity struct {
	RedisObject interface{} // 字符串 跳表 链表 quicklist 集合 etc...
}
