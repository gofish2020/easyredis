package protocal

type Reply interface {
	ToBytes() []byte
}
