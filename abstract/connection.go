package abstract

type Connection interface {
	GetDBIndex() int
	SetDBIndex(int)
	SetPassword(string)
	GetPassword() string
	Write([]byte) (int, error)

	IsClosed() bool
	// pub/sub
	Subscribe(channel string)
	Unsubscribe(channel string)
	SubCount() int
	GetChannels() []string

	// transaction

	IsTransaction() bool
	SetTransaction(bool)

	EnqueueCmd(redisCommand [][]byte)
	GetQueuedCmdLine() [][][]byte

	GetWatchKey() map[string]int64
	CleanWatchKey()

	AddTxError(err error)
	GetTxErrors() []error
}
