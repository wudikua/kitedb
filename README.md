# 一.简介
* 这是一个基于磁盘嵌入式数据库，可以在这个上面进行二次开发

# 二.使用KiteDB的例子

```
db := NewKiteDB("/Users/mengjun/dev/src/kitedb/db/data")
session := db.GetSession()
session.SelectDB("test")
N := 100
begin := time.Now().UnixNano()
for i := 0; i < N; i++ {
	session.Save(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("abc%d", i)))
}
log.Println("wait for async write")
session.Flush()
log.Println("flush end")
```

# 三.性能测试

```
go test -run="TestSave"
2015/03/01 14:42:55 save  1000 record use 93 ms 10688.419 qps/s
PASS
ok  	kitedb/db	3.040s
```

# 四.主要组件
## 1.KiteDB
* 这是一个K-V的数据库，通过如下接口来获取数据库会话

```
GetSession() *KiteDBSession
```
* 获取到Session以后，通过如下接口来存取数据
```
SelectDB(dbName string)
Query(key string) []byte
Save(key string, value []byte) bool
```

## 2.KiteDBPageFile
* 这是一个按照固定大小（1K）分页的文件系统，他将逻辑地址，转换成了物理地址方便来使用

```
Allocate(count int) []*KiteDBPage 
Read(pageIds []int) (pages []*KiteDBPage)
Write(pages []*KiteDBPage)
```

## 3.KiteDBPage
* 这是KiteDBPageFile中管理的每一页，包含如下属性

```
type KiteDBPage struct {
	pageId   int    // 每页一个id
	checksum uint32 // 这页数据的校验和，保证消息的完整性
	pageType int    // 这块数据后续还有页，还是已经包含全部数据
	next     int    // 这页的下一页的位置
	data     []byte // 这页的数据
}
```

## 4.KiteBTreeIndex
* 这是一个BTree索引，参照算法导论上的实现写的，在KiteDB中用它来索引数据的PageId，这个接口也可以独立使用，因为他的被索引项可以是任意数据类型，只要实现了KiteIndexItem接口

```
type KiteIndex interface {
	Insert(key string, data KiteIndexItem) error
	Search(key string) ([]byte, error)
}
```

## 5.KiteBitsetDisk
* 这是一个基于磁盘的Bitset，他在KiteDBPageFile中用来标记了哪些页是可用的，哪些页是不可用的
```
type KiteBitset interface {
	AppendBytes(data []byte)
	Set(index int, status bool)
	At(index int) bool
	Next() int
}
```

## 6.其他数据结构
* 对于KiteIndex还有一个实现是基于redis的KiteBitsetRedis，对于KiteBitset还有一个实现也是基于redis的叫KiteBitsetRedis，还有一个固定大小的环状队列KiteRingQueue


# 四.TODO
* PageFile 应该是基于日志写
* BTree 的删除
* BTree 中数据大于1页以后的多页支持
* Bitset 减少随机写
* Bitset 对于回收空闲页的处理

