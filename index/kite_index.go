package index

type KiteIndex interface {
	Insert(key string, data KiteIndexItem) error
	Search(key string) ([]byte, error)
}

type KiteIndexItem interface {
	Marshal() []byte
	Unmarshal(b []byte) error
	GetData() interface{}
}
