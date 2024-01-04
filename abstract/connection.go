package abstract

type Connection interface {
	GetDBIndex() int
	SetDBIndex(int)
	SetPassword(string)
	GetPassword() string
}
