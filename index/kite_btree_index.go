package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"kitedb/page"
	"log"
	"math"
)

type KiteBTreeNode struct {
	pageId int
	// Contains at least t - 1 keys. Most contains 2t - 1 keys
	keys       []string
	keysPageId []int
	// the keys ptr data
	vals       [][]byte
	valsPageId []int
	// Contains at least t nodes. Most contains 2t nodes
	children []*KiteBTreeNode
	// the minimum node contains
	t int
	// contains n children nodes
	n int
	// this node is a leaf node
	leaf bool
	// btree
	btree *KiteBTreeIndex
	// loaded
	loaded bool
}

type KiteBTreeIndex struct {
	idxName  string
	degree   int
	pageFile *page.KiteDBPageFile
	root     *KiteBTreeNode
}

func NewKiteBTreeIndex(base string, idxName string, degree int) *KiteBTreeIndex {
	btree := &KiteBTreeIndex{
		degree:   degree,
		root:     NewKiteBTreeNode(degree / 2),
		idxName:  idxName,
		pageFile: page.NewKiteDBPageFile(base, idxName),
	}
	btree.root.btree = btree
	pages := btree.pageFile.Read([]int{0})
	first := pages[0]
	if len(first.GetData()) == 0 {
		btree.root.pageId = btree.pageFile.Allocate(1)[0].GetPageId()
	} else {
		log.Println("reopen")
		btree.root.ReadDisk()
	}
	// log.Fatal(btree.root)
	return btree
}

func NewKiteBTreeNode(t int) *KiteBTreeNode {
	return &KiteBTreeNode{
		keys:       make([]string, 2*t-1),
		keysPageId: make([]int, 2*t-1),
		vals:       make([][]byte, 2*t-1),
		valsPageId: make([]int, 2*t-1),
		children:   make([]*KiteBTreeNode, 2*t),
		t:          t,
		n:          0,
		leaf:       true,
		loaded:     false,
	}
}

func (self *KiteBTreeNode) String() string {
	s := ""
	for i, b := range self.children {
		if b != nil && b.n != 0 {
			s = s + fmt.Sprintf("\tbtree %d node children[%d]:%v\n", self.pageId, i, b)
		}
	}
	// for i, b := range self.vals {
	// 	if b != nil {
	// 		s = s + fmt.Sprintf("\tbtree %d vals[%d]:%v\n", self.pageId, i, b)
	// 	}
	// }
	return fmt.Sprintf("btree node\n\tpageid:%d\n\tkeys:%s\n\tt:%d\n\tn:%d\n\tleaf:%t\n", self.pageId, self.keys, self.t, self.n, self.leaf) + s
}

func (self *KiteBTreeNode) WriteDisk() error {
	// log.Println("write node ", self)
	bs := make([]byte, 0, page.PAGE_FILE_PAGE_SIZE)
	buff := bytes.NewBuffer(bs)
	// 有多少个记录
	binary.Write(buff, binary.BigEndian, uint32(self.n))
	binary.Write(buff, binary.BigEndian, uint32(self.t))
	if self.leaf {
		binary.Write(buff, binary.BigEndian, byte(1))
	} else {
		binary.Write(buff, binary.BigEndian, byte(0))
	}
	// log.Println("write n", self.n, " at page ", self.pageId)
	// log.Println("write keys vals", self.keys, self.vals, " at page ", self.pageId)
	// 写入keys vals
	for i := 0; i < self.n && i < 2*self.t-1; i++ {
		binary.Write(buff, binary.BigEndian, uint32(self.keysPageId[i]))
		// log.Println("write key val ", self.keys[i], self.vals[i])
		// log.Println("write keysId", self.keysPageId[i], " at page ", self.pageId)
		binary.Write(buff, binary.BigEndian, uint32(self.valsPageId[i]))
		// log.Println("write valsId", self.valsPageId[i], " at page ", self.pageId)
	}
	if !self.leaf {
		// log.Println("leaf write", self)
		// 写入children
		for i := 0; i < self.n+1 && i < 2*self.t; i++ {
			if self.children[i].pageId == 0 && self.children[i].keys[0] != "" {
				log.Fatal("child pageId can not be zero ", self)
			}
			binary.Write(buff, binary.BigEndian, uint32(self.children[i].pageId))
		}
	}

	pages := []*page.KiteDBPage{page.NewKiteDBPage()}
	pages[0].SetPageId(self.pageId)
	pages[0].SetData(buff.Bytes())
	pages[0].SetChecksum()
	pages[0].SetPageType(page.PAGE_TYPE_END)
	self.btree.pageFile.Write(pages)
	self.btree.pageFile.Flush()
	self.loaded = true
	// log.Println("flush btree", self.pageId)
	return nil
}

func (self *KiteBTreeNode) writeVariable(write []byte) int {
	length := len(write)
	pageN := math.Ceil(float64(length) / float64(self.btree.pageFile.PageSize-page.PAGE_HEADER_SIZE))
	pages := self.btree.pageFile.Allocate(int(pageN))
	for j := 0; j < len(pages); j++ {
		if length < (j+1)*(self.btree.pageFile.PageSize-page.PAGE_HEADER_SIZE) {
			pages[j].SetData(write[j*(self.btree.pageFile.PageSize-page.PAGE_HEADER_SIZE) : length])
		} else {
			pages[j].SetData(write[j*(self.btree.pageFile.PageSize-page.PAGE_HEADER_SIZE) : (j+1)*(self.btree.pageFile.PageSize-page.PAGE_HEADER_SIZE)])
		}

		if j+1 < len(pages) {
			pages[j].SetPageType(page.PAGE_TYPE_PART)
		} else {
			pages[j].SetPageType(page.PAGE_TYPE_END)
		}
		pages[j].SetChecksum()
		// log.Println("page alloc end ", pages[i])
	}
	// 没有写入磁盘，只是放入到了写入队列，同时放到PageCache里
	// log.Println("write ", pages)
	self.btree.pageFile.Write(pages)
	self.btree.pageFile.Flush()
	return pages[0].GetPageId()
}

func (self *KiteBTreeNode) ReadDisk() error {
	if self.loaded {
		return nil
	}
	pages := self.btree.pageFile.Read([]int{self.pageId})
	first := pages[0]
	bs := first.GetData()
	var b byte
	var i32 uint32
	buff := bytes.NewBuffer(bs)
	// 有多少个记录
	binary.Read(buff, binary.BigEndian, &i32)
	self.n = int(i32)
	binary.Read(buff, binary.BigEndian, &i32)
	self.t = int(self.t)
	binary.Read(buff, binary.BigEndian, &b)
	if b == 1 {
		self.leaf = true
	} else {
		self.leaf = false
	}
	for i := 0; i < self.n; i++ {
		binary.Read(buff, binary.BigEndian, &i32)
		key := self.btree.pageFile.Read([]int{int(i32)})
		self.keys[i] = string(key[0].GetData())
		self.keysPageId[i] = int(i32)
		binary.Read(buff, binary.BigEndian, &i32)
		val := self.btree.pageFile.Read([]int{int(i32)})
		self.vals[i] = val[0].GetData()
		self.valsPageId[i] = int(i32)
	}
	if !self.leaf {
		// 读入children
		for i := 0; i < self.n+1 && i < 2*self.t; i++ {
			binary.Read(buff, binary.BigEndian, &i32)
			self.children[i] = NewKiteBTreeNode(self.t)
			self.children[i].pageId = int(i32)
			self.children[i].btree = self.btree
			if self.children[i].pageId != 0 {
				// log.Println("disk read pageid ", self.children[i].pageId)
				// log.Println("disk read ", self.children[i])
				self.children[i].ReadDisk()
			}
		}
	}
	self.loaded = true
	return nil
}

func (self *KiteBTreeNode) findKeyInsert(key string) int {
	var i int
	for i = self.n; i >= 1 && key < self.keys[i-1]; i-- {
		// 插入位置i之后的元素，全部向后移动一个位置
		self.keys[i] = self.keys[i-1]
		self.keysPageId[i] = self.keysPageId[i-1]
		self.vals[i] = self.vals[i-1]
		self.valsPageId[i] = self.valsPageId[i-1]
	}
	// 在第i的元素的位置插入
	return i
}

func (self *KiteBTreeNode) InsertNotFull(key string, data KiteIndexItem) {
	var i int
	if self.leaf {
		// 当前是叶子节点 找到插入位置以后直接插入 @todo 可以改造成二分查找
		i = self.findKeyInsert(key)
		pageKey := self.writeVariable([]byte(key))
		// log.Println("pagekey", pageKey)
		pageVal := self.writeVariable(data.Marshal())
		// log.Println("pageval", pageVal)
		// log.Println("cur i", i)
		self.keys[i] = key
		self.vals[i] = data.Marshal()
		self.keysPageId[i] = pageKey
		self.valsPageId[i] = pageVal

		self.n += 1
	} else {
		// log.Println("need search children and insert")
		for i = self.n - 1; i >= 0 && key < self.keys[i]; i-- {
		}
		i += 1
		self.children[i].ReadDisk()
		if self.children[i].n == self.t*2-1 {
			// log.Println("child need split")
			// 子树满 先分裂
			self.btree.BTreeSplitChild(self, self.children[i], i)
			if key > self.keys[i] {
				i++
			}
		}
		// log.Println("find insert into children ", self.children[i])
		self.children[i].InsertNotFull(key, data)
	}
	self.WriteDisk()
}

func (self *KiteBTreeIndex) BTreeSplitChild(node *KiteBTreeNode, child *KiteBTreeNode, pos int) {
	rchild := NewKiteBTreeNode(self.degree / 2)
	rchild.pageId = self.pageFile.Allocate(1)[0].GetPageId()
	rchild.leaf = child.leaf
	rchild.n = self.degree/2 - 1
	rchild.btree = self
	// 从原来的根节点拷贝数据到右节点
	for i := 0; i < self.degree/2-1; i++ {
		rchild.keys[i] = child.keys[self.degree/2+i]
		rchild.vals[i] = child.vals[self.degree/2+i]
		rchild.keysPageId[i] = child.keysPageId[self.degree/2+i]
		rchild.valsPageId[i] = child.valsPageId[self.degree/2+i]
	}
	child.n = self.degree/2 - 1
	if !child.leaf {
		for i := 0; i < self.degree/2; i++ {
			rchild.children[i] = child.children[self.degree/2+i]
		}
	}

	// log.Println("rchild ", rchild)
	// 从原来的根节点拷贝数据到新的根节点
	// 新的根节点数据从结尾到插入位置右移一位 为了空出rchild的插入位置
	for i := node.n; i > pos; i-- {
		node.children[i+1] = node.children[i]
	}

	// 新的根节点上插入右节点为自己的子节点
	node.children[pos+1] = rchild

	// 移动完子树移动关键字
	for i := node.n - 1; i >= pos; i-- {
		node.keys[i+1] = node.keys[i]
		node.vals[i+1] = node.vals[i]
		node.keysPageId[i+1] = node.keysPageId[i]
		node.valsPageId[i+1] = node.valsPageId[i]
	}
	node.keys[pos] = child.keys[self.degree/2-1]
	node.keysPageId[pos] = child.keysPageId[self.degree/2-1]
	node.vals[pos] = child.vals[self.degree/2-1]
	node.valsPageId[pos] = child.valsPageId[self.degree/2-1]
	node.n += 1
	// log.Fatal("root ", node)
	// 删掉以前老的根节点的数据
	for i := child.n; i < self.degree-1; i++ {
		child.keys[i] = ""
		child.vals[i] = nil
		child.keysPageId[i] = 0
		child.valsPageId[i] = 0
	}
	for i := child.n + 1; i < self.degree; i++ {
		child.children[i] = NewKiteBTreeNode(self.degree / 2)
	}
	// log.Fatal("split root ", node)

	node.WriteDisk()
	child.WriteDisk()
	rchild.WriteDisk()
}

func (self *KiteBTreeNode) Search(key string) []byte {
	self.ReadDisk()
	if self.n == 0 {
		return nil
	}
	var i int
	for i = self.n - 1; i >= 0 && key < self.keys[i]; i-- {
	}
	// log.Println("compare ", key, self.keys[i], " at ", i)
	if i != -1 && self.keys[i] == key {
		return self.vals[i]
	}
	i += 1
	if self.leaf == true {
		return nil
	}
	return self.children[i].Search(key)
}

func (self *KiteBTreeIndex) Insert(key string, data KiteIndexItem) error {
	r := self.root
	if r.n == 2*r.t-1 {
		// 根节点已经满了，需要树增高分裂
		// log.Println("root full")
		root := NewKiteBTreeNode(self.degree / 2)
		root.btree = self
		root.pageId = r.pageId
		root.leaf = false
		root.children[0] = r
		r.pageId = self.pageFile.Allocate(1)[0].GetPageId()
		self.BTreeSplitChild(root, r, 0)
		root.InsertNotFull(key, data)
		self.root = root
	} else {
		// 直接在节点上插入
		r.InsertNotFull(key, data)
	}
	return nil
}

func (self *KiteBTreeIndex) Search(key string) ([]byte, error) {
	if self.root == nil {
		return nil, nil
	}
	var i int
	for i = self.root.n - 1; i >= 0 && key < self.root.keys[i]; i-- {
	}
	if i != -1 && self.root.keys[i] == key {
		return self.root.vals[i], nil
	}
	i += 1
	if self.root.leaf == true {
		return nil, nil
	}
	return self.root.children[i].Search(key), nil
}

func (self *KiteBTreeIndex) Display() string {
	return fmt.Sprintf("btree index\nname:%s\ndegree:%d\nroot:%v\n", self.idxName, self.degree, self.root)
}
