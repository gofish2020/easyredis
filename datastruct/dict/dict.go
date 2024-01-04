package dict

type Consumer func(key string, val interface{}) bool
