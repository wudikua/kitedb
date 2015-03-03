package db

import (
	// "errors"
	"kitedb/index"
	"kitedb/index/item"
	"kitedb/page"
	// "log"
	// "fmt"
	"math"
)

// 一个存储引擎
type KiteDB struct {
	dbs  map[string]*page.KiteDBPageFile
	idxs map[string]index.KiteIndex
	dir  string
}

// 创建一个DB，指定一个存储目录
func NewKiteDB(dir string) *KiteDB {
	ins := &KiteDB{
		dir:  dir,
		dbs:  make(map[string]*page.KiteDBPageFile),
		idxs: make(map[string]index.KiteIndex),
	}
	return ins
}

func (self *KiteDB) SelectDB(dbName string) (*page.KiteDBPageFile, index.KiteIndex, error) {
	var db *page.KiteDBPageFile
	var idx index.KiteIndex
	db, exists := self.dbs[dbName]
	if !exists {
		db = page.NewKiteDBPageFile(self.dir, dbName)
		self.dbs[dbName] = db
	}

	idx, exists = self.idxs[dbName]
	if !exists {
		// idx = index.NewRedisIndex(dbName)
		idx = index.NewKiteBTreeIndex(self.dir+"/index", dbName, 64, false)
		self.idxs[dbName] = idx
	}
	return db, idx, nil
}

func (self *KiteDB) FlushDB(dbName string) {
	pageFile, _, _ := self.SelectDB(dbName)
	pageFile.Flush()
}

func (self *KiteDB) GetSession() *KiteDBSession {
	return &KiteDBSession{
		db: self,
	}
}

// 一次数据库会话
type KiteDBSession struct {
	db       *KiteDB
	pageFile *page.KiteDBPageFile
	index    index.KiteIndex
}

func (self *KiteDBSession) SelectDB(dbName string) {
	self.pageFile, self.index, _ = self.db.SelectDB(dbName)
}

func (self *KiteDBSession) Flush() {
	self.pageFile.Flush()
}

func (self *KiteDBSession) Query(key string) []byte {
	indexData, _ := self.index.Search(key)
	query := &item.KeyIndexItem{}
	query.Unmarshal(indexData)
	return self.pageFile.ReadSeqData(query.PageId)
}

func (self *KiteDBSession) Save(key string, value []byte) bool {
	length := len(value)
	var bs []byte
	pageN := math.Ceil(float64(length) / float64(self.pageFile.PageSize-page.PAGE_HEADER_SIZE))

	// log.Println("page alloc ", pageN)
	pages := self.pageFile.Allocate(int(pageN))
	for i := 0; i < len(pages); i++ {
		if length < (i+1)*(self.pageFile.PageSize-page.PAGE_HEADER_SIZE) {
			bs = make([]byte, length-(i)*(self.pageFile.PageSize-page.PAGE_HEADER_SIZE))
			copy(bs, value[(i)*(self.pageFile.PageSize-page.PAGE_HEADER_SIZE):length])
		} else {
			bs = make([]byte, self.pageFile.PageSize-page.PAGE_HEADER_SIZE)
			copy(bs, value[i*(self.pageFile.PageSize-page.PAGE_HEADER_SIZE):(i+1)*(self.pageFile.PageSize-page.PAGE_HEADER_SIZE)])
		}
		pages[i].SetData(bs)

		if i+1 < len(pages) {
			pages[i].SetPageType(page.PAGE_TYPE_PART)
			pages[i].SetNext(pages[i+1].GetPageId())
		} else {
			pages[i].SetPageType(page.PAGE_TYPE_END)
		}
		pages[i].SetChecksum()
		// log.Println("page alloc end ", pages[i])
	}
	// 没有写入磁盘，只是放入到了写入队列，同时放到PageCache里
	// log.Println("write ", pages)
	self.pageFile.Write(pages)
	// @todo 建立messageId到pageId,topic的索引
	self.index.Insert(key, &item.KeyIndexItem{
		PageId: pages[0].GetPageId(),
	})
	return true
}

func (self *KiteDBSession) Update(key string, value []byte) bool {
	indexData, _ := self.index.Search(key)
	query := &item.KeyIndexItem{}
	query.Unmarshal(indexData)
	pages := self.pageFile.ReadSeqPages(query.PageId)
	pagesCount := len(pages)
	length := len(value)
	var bs []byte
	pageN := int(math.Ceil(float64(length) / float64(self.pageFile.PageSize-page.PAGE_HEADER_SIZE)))
	if pagesCount > pageN {
		for i := len(pages) - 1; i >= pageN; i-- {
			self.pageFile.Free([]int{pages[i].GetPageId()})
			pages[i] = nil
			pagesCount -= 1
		}
	} else if pagesCount < pageN {
		appendPages := self.pageFile.Allocate(pageN - len(pages))
		for _, p := range appendPages {
			pages = append(pages, p)
			pagesCount += 1
		}
	}

	for i := 0; i < pageN; i++ {
		if length < (i+1)*(self.pageFile.PageSize-page.PAGE_HEADER_SIZE) {
			bs = make([]byte, length-(i)*(self.pageFile.PageSize-page.PAGE_HEADER_SIZE))
			copy(bs, value[(i)*(self.pageFile.PageSize-page.PAGE_HEADER_SIZE):length])
		} else {
			bs = make([]byte, self.pageFile.PageSize-page.PAGE_HEADER_SIZE)
			copy(bs, value[i*(self.pageFile.PageSize-page.PAGE_HEADER_SIZE):(i+1)*(self.pageFile.PageSize-page.PAGE_HEADER_SIZE)])
		}
		pages[i].SetData(bs)

		if i+1 < pagesCount {
			pages[i].SetPageType(page.PAGE_TYPE_PART)
			pages[i].SetNext(pages[i+1].GetPageId())
		} else {
			pages[i].SetPageType(page.PAGE_TYPE_END)
		}
		pages[i].SetChecksum()
	}
	self.pageFile.Write(pages)
	return true
}
