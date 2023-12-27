# Golang实现自己的redis

本项目分成 11 个部分，实现一个可用的Redis服务，姑且叫**EasyRedis**吧，希望通过文章将Redis掰开撕碎了让大家有更直观的理解，而不是仅仅停留在八股文的层面，而是非常爽的感觉，欢迎持续关注学习。

## 单机版
- [x] [easyredis之TCP服务](https://github.com/gofish2020/easyredis/blob/main/doc/1.tcp%E6%9C%8D%E5%8A%A1/tcp%E6%9C%8D%E5%8A%A1.md) 
- [x] [easyredis之网络请求序列化协议（RESP）](https://github.com/gofish2020/easyredis/blob/main/doc/2.Redis%E5%BA%8F%E5%88%97%E5%8C%96%E5%8D%8F%E8%AE%AE/RESP.md)
- [ ] easyredis之内存数据库
- [ ] easyredis之过期时间 (时间轮实现)
- [ ] easyredis之持久化 （AOF实现）
- [ ] easyredis之发布订阅功能
- [ ] easyredis之有序集合（跳表实现）
- [ ] easyredis之 pipeline 客户端实现
- [ ] easyredis之事务（原子性/回滚）

## 分布式
- [ ] easyredis之连接池
- [ ] easyredis之分布式集群存储
