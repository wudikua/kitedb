package index

import (
	"fmt"
	"github.com/xuyu/goredis"
	"log"
)

type KiteRedisIndex struct {
	redis   *goredis.Redis
	idxName string
}

func NewRedisIndex(idxName string) *KiteRedisIndex {
	redis, err := goredis.Dial(&goredis.DialConfig{Address: ":6379"})
	if err != nil {
		log.Fatal("make sure indexing redis is alive")
	}
	ins := &KiteRedisIndex{
		redis:   redis,
		idxName: idxName,
	}
	return ins
}

func (self *KiteRedisIndex) Insert(key string, data KiteIndexItem) error {
	self.redis.ExecuteCommand("SET",
		fmt.Sprintf("%s_%s", self.idxName, key), data.Marshal())
	return nil
}

func (self *KiteRedisIndex) Search(key string) ([]byte, error) {
	r, _ := self.redis.ExecuteCommand("GET",
		fmt.Sprintf("%s_%s", self.idxName, key))
	b, _ := r.BytesValue()
	return b, nil
}
