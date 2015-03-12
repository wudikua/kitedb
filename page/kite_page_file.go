package page

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"kitedb/util"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

const PAGEFILE_SUFFIX = ".data"
const PAGE_FILE_HEADER_SIZE = 4 * 1024
const NEW_FREE_LIST_SIZE = 32
const PAGE_FILE_PAGE_COUNT = 10240
const PAGE_FILE_PAGE_SIZE = 1 * 1024
const MAX_PAGE_FILES = 32
const PAGE_FILE_PAGE_CACHE_SIZE = 1024

// 维护了一组数据文件
type KiteDBPageFile struct {
	path             string
	writeFile        map[int]*os.File
	readFile         map[int]*os.File
	PageSize         int                //每页的大小 默认4K
	pageCount        int                //每个PageFile文件包含page数量
	pageCache        *util.KiteLRUCache //以页ID为索引的缓存
	pageCacheSize    int                //页缓存大小
	writes           chan *KiteDBWrite  //刷盘队列
	writeStop        chan int
	writeFlush       chan int
	writeFlushFinish chan int
	pageStatus       util.KiteBitset
	allocLock        sync.Mutex //主要是对分配Page的时候需要加锁，防止重复分配了一个Page
	freeList         *list.List //空闲页 优先向这里写入
}

// 数据的写入做了个封装
type KiteDBWrite struct {
	page   *KiteDBPage //写到哪个页上
	data   []byte      //写的数据
	time   int64
	length int //写入长度
}

func NewKiteDBPageFile(base string, dbName string) *KiteDBPageFile {
	dir := fmt.Sprintf("%s/%s", base, dbName)
	log.Println(dir)
	// 创建目录
	if _, err := os.Stat(dir); err != nil {
		if err := os.MkdirAll(dir, 0777); err != nil {
			log.Fatal("create ", dir, " failed")
			return nil
		}
	}
	var mutex sync.Mutex
	ins := &KiteDBPageFile{
		path:             dir,
		readFile:         make(map[int]*os.File),
		writeFile:        make(map[int]*os.File),
		PageSize:         PAGE_FILE_PAGE_SIZE,
		pageCount:        PAGE_FILE_PAGE_COUNT,
		pageCache:        util.NewKiteLRUCache(PAGE_FILE_PAGE_CACHE_SIZE),
		pageCacheSize:    PAGE_FILE_PAGE_CACHE_SIZE,
		writes:           make(chan *KiteDBWrite, 1),
		pageStatus:       util.NewKiteBitsetDisk(dir, dbName),
		freeList:         list.New(),
		allocLock:        mutex,
		writeStop:        make(chan int, 1),
		writeFlush:       make(chan int, 1),
		writeFlushFinish: make(chan int, 1),
	}
	go ins.pollWrite()
	return ins
}

func (self *KiteDBPageFile) reAllocFreeList(size int) {
	for i := 0; i < size; i++ {
		self.freeList.PushFront(&KiteDBPage{
			pageId: self.pageStatus.Next() + i,
		})
	}
}

func (self *KiteDBPageFile) Allocate(count int) []*KiteDBPage {
	self.allocLock.Lock()
	defer self.allocLock.Unlock()
	// log.Println("create ", count, "pages")
	pages := make([]*KiteDBPage, count)
	// log.Println("create pages", pages)
	for i := 0; i < count; i = i + 1 {
		if self.freeList.Len() == 0 {
			// 重新分配新的freeList
			self.reAllocFreeList(NEW_FREE_LIST_SIZE)
		}
		e := self.freeList.Back()
		pages[i] = e.Value.(*KiteDBPage)
		self.freeList.Remove(e)
		// 代表页已经被占用
		self.pageStatus.Set(pages[i].pageId, true)
		// log.Println("allocId", pages[i].pageId)
	}
	// log.Println("create pages result", pages)
	return pages
}

func (self *KiteDBPageFile) Free(pageIds []int) {
	self.allocLock.Lock()
	defer self.allocLock.Unlock()
	for _, pageId := range pageIds {
		self.freeList.PushFront(&KiteDBPage{
			pageId: pageId,
		})
		self.pageStatus.Set(pageId, false)
		_, contains := self.pageCache.Get(pageId)
		if contains {
			self.pageCache.Delete(pageId)
		}
	}
}

func (self *KiteDBPageFile) Read(pageIds []int) (pages []*KiteDBPage) {
	result := []*KiteDBPage{}
	for _, pageId := range pageIds {
		pageInterface, contains := self.pageCache.Get(pageId)
		var page *KiteDBPage
		if !contains {
			// log.Println("miss page cache")
			page = &KiteDBPage{
				pageId: pageId,
			}
			no := page.getWriteFileNo()
			file := self.readFile[no]
			// log.Println("write file no", no, file)
			if file == nil {
				file, _ = os.OpenFile(
					fmt.Sprintf("%s/%d%s", self.path, no, PAGEFILE_SUFFIX),
					os.O_CREATE|os.O_RDWR,
					0666)
				self.readFile[no] = file
			}
			file.Seek(page.getOffset(), 0)
			if err := page.ToPage(file); err != nil {
				log.Fatal(err)
			}
			self.pageCache.Set(pageId, page)
		} else {
			page = pageInterface.(*KiteDBPage)
		}
		// log.Println("fetch page from cache", page.data)
		result = append(result, page)
	}
	return result
}

func (self *KiteDBPageFile) ReadSeqData(pageId int) []byte {
	val := self.Read([]int{pageId})
	// log.Println("next", val[0].GetNext())
	if val[0].GetPageType() == PAGE_TYPE_END {
		// 单页
		return val[0].GetData()
	} else {
		// 多页
		buff := bytes.NewBuffer(val[0].GetData())
		for val[0].GetPageType() == PAGE_TYPE_PART {
			val = self.Read([]int{val[0].GetNext()})
			// log.Println("next", val[0].GetNext(), val[0].GetPageType())
			// log.Println("data length", len(val[0].GetData()))
			buff.Write(val[0].GetData())
		}
		return buff.Bytes()
	}
}

func (self *KiteDBPageFile) ReadSeqPages(pageId int) []*KiteDBPage {
	result := make([]*KiteDBPage, 1)
	val := self.Read([]int{pageId})
	result[0] = &KiteDBPage{
		pageId: pageId,
	}
	if val[0].GetPageType() == PAGE_TYPE_END {
		return result
	} else {
		for val[0].GetPageType() != PAGE_TYPE_PART {
			result = append(result, val[0])
			val = self.Read([]int{val[0].GetNext()})
		}
	}
	result = append(result, val[0])
	return result
}

func (self *KiteDBPageFile) Write(pages []*KiteDBPage) {
	for _, page := range pages {
		if page == nil {
			continue
		}
		// 写page缓存
		self.pageCache.Set(page.pageId, page)
		// log.Println("write page cache", page.pageId, page.data)
		// log.Println("write async")
		self.writes <- &KiteDBWrite{
			page:   page,
			data:   page.data,
			time:   time.Now().UnixNano(),
			length: len(page.data),
		}
	}
}

func (self *KiteDBPageFile) writeHeader(writer *os.File) error {
	buffer := make([]byte, PAGE_HEADER_SIZE)
	buff := bytes.NewBuffer(buffer)
	// @todo 还没想到写什么好
	binary.Write(buff, binary.BigEndian, "KITE")
	_, err := writer.Write(buff.Bytes())
	return err
}

func (self *KiteDBPageFile) validHeader(reader *io.Reader) bool {
	buffer := make([]byte, 4)
	buff := bytes.NewBuffer(buffer)
	var data string
	binary.Read(buff, binary.BigEndian, &data)
	return data == "KITE"
}

type KiteDBWriteBatch []*KiteDBWrite

func (self KiteDBWriteBatch) Len() int {
	return len(self)
}

func (self KiteDBWriteBatch) Less(i, j int) bool {
	if self[i].page.pageId == self[j].page.pageId {
		return self[i].time > self[j].time
	}
	return self[i].page.pageId < self[j].page.pageId
}

func (self KiteDBWriteBatch) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self *KiteDBPageFile) pollWrite() {
	writeStart := make(chan int, 1)
	writeQueue := make(chan KiteDBWriteBatch)
	list := make(KiteDBWriteBatch, 0, 1024)
	flush := make(chan int, 1)
	benchSize := 1024
	go self.WriteBatch(writeQueue, flush)

	go func() {
		// 批量写的策略
		for {
			time.Sleep(time.Second * 1)
			writeStart <- 1
		}
	}()
	for {
		select {
		case <-self.writeStop:
			return
		case <-self.writeFlush:
			if len(self.writes) != 0 || len(list) != 0 {
				clone := make(KiteDBWriteBatch, len(list))
				copy(clone, list[:len(list)])
				writeQueue <- clone
				flush <- 1
				list = make(KiteDBWriteBatch, 0, benchSize)
			}
			flush <- 1
			self.writeFlushFinish <- 1
		case <-writeStart:
			// log.Println("write queue active", list)
			clone := make(KiteDBWriteBatch, len(list))
			copy(clone, list[:len(list)])
			writeQueue <- clone
			list = make(KiteDBWriteBatch, 0, benchSize)
		case pageWrite := <-self.writes:
			// log.Println("append page write ", pageWrite.page.data)
			if len(list) == benchSize {
				clone := make(KiteDBWriteBatch, len(list))
				copy(clone, list[:len(list)])
				writeQueue <- clone
				list = make(KiteDBWriteBatch, 0, benchSize)
			}
			list = append(list, pageWrite)
		}
	}
}

func (self *KiteDBPageFile) Flush() {
	self.writeFlush <- 1
	<-self.writeFlushFinish
	// log.Println("flush end")
}

func (self *KiteDBPageFile) getWriteFile(no int) *os.File {
	file := self.writeFile[no]
	// log.Println("write file no", no, file)
	if file == nil {
		file, _ = os.OpenFile(
			fmt.Sprintf("%s/%d%s", self.path, no, PAGEFILE_SUFFIX),
			os.O_CREATE|os.O_RDWR,
			0666)
		self.writeFile[no] = file
		fileStat, _ := file.Stat()
		if fileStat.Size() == 0 {
			self.writeHeader(file)
		}
	}
	return file
}

func (self *KiteDBPageFile) doWrite(l KiteDBWriteBatch) {
	if len(l) == 0 {
		return
	}
	sort.Sort(l)
	filterIndex := 0
	filterList := make([]*KiteDBWrite, 1)
	filterList[0] = l[0]
	for _, page := range l {
		if filterList[filterIndex].page.pageId == page.page.pageId {
			continue
		}
		filterIndex += 1
		filterList = append(filterList, page)
	}
	length := len(filterList)
	for i := 0; i < length; i++ {
		page := filterList[i]

		no := page.page.getWriteFileNo()

		file := self.getWriteFile(no)

		bs := page.page.ToBinary()

		file.Seek(page.page.getOffset(), 0)

		if i+1 < length && filterList[i+1].page.pageId == page.page.pageId+1 {
			buff := bytes.NewBuffer(bs)
			for j := 1; j+i < length; j++ {
				if filterList[j].page.pageId == page.page.pageId+j {
					buff.Write(filterList[j].page.ToBinary())
				} else {
					break
				}
				i = j
			}

			_, err := file.Write(buff.Bytes())
			if err != nil {
				log.Fatal(err)
			}
		} else {
			_, err := file.Write(bs)
			if err != nil {
				log.Fatal(err)
			}
		}

		// log.Println("write binary", bs)
		// log.Println("write binary length", len(bs))

		// file.Sync()
		// log.Println("write end ", self.path, page.page.pageId)
		// log.Println("write ", n, " bytes")
	}
	self.pageStatus.Flush()
	// log.Fatal("write a batch")
}

func (self *KiteDBPageFile) WriteBatch(queue chan KiteDBWriteBatch, flush chan int) {
	for {
		select {
		case <-self.writeStop:
			return
		case <-flush:
			for len(queue) > 0 {
				l := <-queue
				self.doWrite(l)
			}
		case l := <-queue:
			self.doWrite(l)
		}
	}
}
