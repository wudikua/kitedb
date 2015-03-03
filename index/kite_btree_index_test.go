package index

import (
	"fmt"
	"kitedb/index/item"
	"log"
	"testing"
	"time"
)

func TestInsert(t *testing.T) {
	// t=2的btree
	idx := NewKiteBTreeIndex("/Users/mengjun/dev/src/kitedb/index", "test", 4, true)
	val := &item.KeyIndexItem{
		PageId: 1,
	}
	key := fmt.Sprintf("%d", 8)
	idx.Insert(key, val)
	log.Println("root", idx.root)
	time.Sleep(time.Second * 2)
}

func TestSearch(t *testing.T) {
	idx := NewKiteBTreeIndex("/Users/mengjun/dev/src/kitedb/index", "test", 4, true)
	rs, _ := idx.Search("99597510")
	log.Println("query result", rs)
}

func TestIndexBench(t *testing.T) {
	N := 100
	var idx *KiteBTreeIndex
	idx = NewKiteBTreeIndex("/Users/mengjun/dev/src/kitedb/index", "test", 4, false)
	for i := 0; i < N; i++ {
		val := &item.KeyIndexItem{
			PageId: i + 1,
		}
		key := fmt.Sprintf("%d", i)
		log.Println("index ", key, i+1)
		idx.Insert(key, val)
	}
	idx.pageFile.Flush()

	log.Println("index ", N, " key val")
	time.Sleep(time.Second * 3)
	// log.Println(idx.root)
}

func TestSearchBench(t *testing.T) {
	N := 100
	var idx *KiteBTreeIndex
	idx = NewKiteBTreeIndex("/Users/mengjun/dev/src/kitedb/index", "test", 4, false)
	// log.Println(idx.root)
	// key := "2"
	// rs, _ := idx.Search(key)
	// log.Println("search ", key, rs)
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("%d", i)
		rs, _ := idx.Search(key)
		log.Println("search ", key, rs)
	}
	log.Println("search ", N, " key")
	time.Sleep(time.Second)
}
