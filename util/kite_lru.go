package util

import (
	"container/list"
	"sync"
)

type KiteLRUCache struct {
	// back端是热端 front端是冷端 的双端队列
	list *list.List
	// 一个kv的map
	kv map[interface{}]*list.Element
	// 大小
	size int
	// 一个协程操作这个cache
	sync.Mutex
}

type playload struct {
	key interface{}
	val interface{}
}

func NewKiteLRUCache(size int) *KiteLRUCache {
	return &KiteLRUCache{
		list: list.New(),
		kv:   make(map[interface{}]*list.Element, size),
		size: size,
	}
}

func (self *KiteLRUCache) Set(key, val interface{}) {
	self.Lock()
	defer self.Unlock()
	if self.kv[key] != nil {
		return
	}
	// 插入热端
	self.list.PushBack(&playload{
		key: key,
		val: val,
	})
	self.kv[key] = self.list.Back()
	// 满了从冷端 pop出一个
	if self.list.Len() > self.size {
		e := self.list.Front()
		delete(self.kv, e.Value)
		self.list.Remove(e)
	}
}

func (self *KiteLRUCache) Get(key interface{}) (interface{}, bool) {
	self.Lock()
	defer self.Unlock()
	e := self.kv[key]
	if e == nil {
		return nil, false
	}
	// 把当前元素移到热端
	self.list.MoveToBack(e)
	return e.Value.(*playload).val, true
}

func (self *KiteLRUCache) Delete(key interface{}) {
	self.Lock()
	defer self.Unlock()
	e := self.kv[key]
	if e == nil {
		return
	}
	delete(self.kv, e.Value)
	self.list.Remove(e)
}
