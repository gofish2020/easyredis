package cluster

import (
	"errors"

	"github.com/gofish2020/easyredis/aof"
	"github.com/gofish2020/easyredis/datastruct/dict"
	"github.com/gofish2020/easyredis/redis/client"
	"github.com/gofish2020/easyredis/redis/protocol"
	"github.com/gofish2020/easyredis/tool/conf"
	"github.com/gofish2020/easyredis/tool/pool"
)

type Factory interface {
	GetConn(addr string) (Client, error)
	ReturnConn(peer string, cli Client) error
}

type Client interface {
	Send(command [][]byte) (protocol.Reply, error)
}

type RedisConnPool struct {
	connDict *dict.ConcurrentDict // addr -> *pool.Pool
}

func NewRedisConnPool() *RedisConnPool {

	return &RedisConnPool{
		connDict: dict.NewConcurrentDict(16),
	}
}

func (r *RedisConnPool) GetConn(addr string) (Client, error) {

	var connectionPool *pool.Pool // 对象池

	// 通过不同的地址addr，获取不同的对象池
	raw, ok := r.connDict.Get(addr)
	if ok {
		connectionPool = raw.(*pool.Pool)
	} else {

		// 创建对象函数
		newClient := func() (any, error) {
			// redis的客户端连接
			cli, err := client.NewRedisClient(addr)
			if err != nil {
				return nil, err
			}
			// 启动
			cli.Start()
			if conf.GlobalConfig.RequirePass != "" { // 说明服务需要密码
				reply, err := cli.Send(aof.Auth([]byte(conf.GlobalConfig.RequirePass)))
				if err != nil {
					return nil, err
				}
				if !protocol.IsOKReply(reply) {
					return nil, errors.New("auth failed:" + string(reply.ToBytes()))
				}
				return cli, nil
			}
			return cli, nil
		}

		// 释放对象函数
		freeClient := func(x any) {
			cli, ok := x.(*client.RedisClient)
			if ok {
				cli.Stop() // 释放
			}
		}

		// 针对addr地址，创建一个新的对象池
		connectionPool = pool.NewPool(newClient, freeClient, pool.Config{
			MaxIdles:  1,
			MaxActive: 20,
		})
		// addr -> *pool.Pool
		r.connDict.Put(addr, connectionPool)
	}

	// 从对象池中获取一个对象
	raw, err := connectionPool.Get()
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.RedisClient)
	if !ok {
		return nil, errors.New("connection pool make wrong type")
	}
	return conn, nil
}

func (r *RedisConnPool) ReturnConn(peer string, cli Client) error {
	raw, ok := r.connDict.Get(peer)
	if !ok {
		return errors.New("connection pool not found")
	}
	raw.(*pool.Pool).Put(cli)
	return nil
}
