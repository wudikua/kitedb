package db

import (
	"fmt"
	"log"
	// "os"
	// "runtime/pprof"
	"strings"
	"testing"
	"time"
)

func TestSave(t *testing.T) {
	// f, err := os.Create("pprof.data")
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// pprof.StartCPUProfile(f)
	db := NewKiteDB("/Users/mengjun/dev/src/kitedb/db/data")
	session := db.GetSession()
	session.SelectDB("test")
	N := 1000
	begin := time.Now().UnixNano()
	for i := 0; i < N; i++ {
		session.Save(fmt.Sprintf("%d", i), []byte(strings.Repeat(fmt.Sprintf("%d", i%10), 200)))
	}
	log.Println("wait for async write")
	session.Flush()
	log.Println("flush end")
	end := time.Now().UnixNano()
	per := float32(float32(end-begin) / float32(1000) / float32(1000) / float32(N))
	log.Println("save ", N, "record use", (end-begin)/1000/1000, "ms", 1000/per, "qps/s")
	// time.Sleep(time.Second * 3)
	// pprof.StopCPUProfile()
}

func TestReSave(t *testing.T) {
	db := NewKiteDB("/Users/mengjun/dev/src/kitedb/db/data")
	session := db.GetSession()
	session.SelectDB("test")
	N := 500
	begin := time.Now().UnixNano()
	for i := 0; i < N; i++ {
		session.Save(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("abc%d", i)))
	}
	log.Println("wait for async write")
	session.Flush()
	log.Println("flush end")
	end := time.Now().UnixNano()
	log.Println("save ", N, "record use", (end-begin)/1000/1000, " ms")

	// 重新打开数据库
	db = NewKiteDB("/Users/mengjun/dev/src/kitedb/db/data")
	session = db.GetSession()
	session.SelectDB("test")
	N = 1000
	begin = time.Now().UnixNano()
	for i := 500; i < N; i++ {
		session.Save(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("abc%d", i)))
	}
	log.Println("wait for async write")
	session.Flush()
	log.Println("flush end")
	end = time.Now().UnixNano()
	log.Println("save ", N, "record use", (end-begin)/1000/1000, " ms")
	// time.Sleep(time.Second * 3)
}

func TestQuery(t *testing.T) {
	db := NewKiteDB("/Users/mengjun/dev/src/kitedb/db/data")
	session := db.GetSession()
	session.SelectDB("test")
	N := 1000
	begin := time.Now().UnixNano()
	for i := 0; i < N; i++ {
		bs := session.Query(fmt.Sprintf("%d", i))
		if strings.Repeat(fmt.Sprintf("%d", i%10), 200) != string(bs) {
			// log.Fatal(len(string(bs)))
			// log.Fatal("query failed", i)
		}
		// log.Println("query result", string(bs))
		log.Println("query success ", i)
	}
	end := time.Now().UnixNano()
	per := float32(float32(end-begin) / float32(1000) / float32(1000) / float32(N))
	log.Println("query ", N, "record use", (end-begin)/1000/1000, "ms", 1000/per, "qps/s")
}

func TestUpdate(t *testing.T) {
	db := NewKiteDB("/Users/mengjun/dev/src/kitedb/db/data")
	session := db.GetSession()
	session.SelectDB("test")
	N := 1000
	begin := time.Now().UnixNano()
	for i := 0; i < N; i++ {
		session.Update(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("abc%d update", i)))
	}
	log.Println("wait for async update")
	session.Flush()
	end := time.Now().UnixNano()
	per := float32(float32(end-begin) / float32(1000) / float32(1000) / float32(N))
	log.Println("update ", N, "record use", (end-begin)/1000/1000, "ms", 1000/per, "qps/s")
}
