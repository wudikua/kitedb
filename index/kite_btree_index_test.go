package index

import (
	"fmt"
	"kitedb/index/item"
	"log"
	"net/http"
	_ "net/http/pprof"
	"testing"
	"time"
)

func TestInsert(t *testing.T) {
	// t=2的btree
	idx := NewKiteBTreeIndex("data", "test", 4, true)
	val := &item.KeyIndexItem{
		PageId: 1,
	}
	key := fmt.Sprintf("%d", 8)
	idx.Insert(key, val)
	log.Println("root", idx.root)
	time.Sleep(time.Second * 2)
}

func TestSearch(t *testing.T) {
	idx := NewKiteBTreeIndex("data", "test", 4, true)
	rs, _ := idx.Search("99597510")
	log.Println("query result", rs)
}

func TestIndexBench(t *testing.T) {
	go func() {
		http.ListenAndServe(":13800", nil)
	}()
	N := 100000
	var idx *KiteBTreeIndex
	idx = NewKiteBTreeIndex("data", "test", 64, false)
	begin := time.Now().UnixNano()
	for i := 0; i < N; i++ {
		val := &item.KeyIndexItem{
			PageId: i + 1,
		}
		key := fmt.Sprintf("%d", i)
		// log.Println("index ", key, i+1)
		idx.Insert(key, val)
	}
	idx.pageFile.Flush()
	end := time.Now().UnixNano()
	per := float32(float32(end-begin) / float32(1000) / float32(1000) / float32(N))
	log.Println("index ", N, "record use", (end-begin)/1000/1000, "ms", 1000/per, "qps/s")
	time.Sleep(time.Second * 30)
	// log.Println(idx.root)
}

func TestSearchBench(t *testing.T) {
	N := 100
	var idx *KiteBTreeIndex
	idx = NewKiteBTreeIndex("data", "test", 64, false)
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
