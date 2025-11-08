package usecase

type Data interface {
	Get(key string) ([]byte, bool)
	Put(key, value []byte)
	Delete(key string)
}