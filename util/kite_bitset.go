package util

import (
	"container/list"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

type KiteBitset interface {
	Set(index int, status bool)
	At(index int) bool
	Next() int
	Flush()
}

type KiteBitsetDisk struct {
	numBits  int
	nextBits int
	bits     []byte
	dir      string
	name     string
	fp       *os.File
	free     *list.List
}

func NewKiteBitsetDisk(dir string, name string) *KiteBitsetDisk {
	var b *KiteBitsetDisk
	b = &KiteBitsetDisk{
		numBits: 0,
		bits:    make([]byte, 0),
		dir:     dir,
		name:    name,
		free:    list.New(),
	}
	b.load()
	return b
}

func (self *KiteBitsetDisk) load() {
	var err error
	if self.fp, err = os.OpenFile(fmt.Sprintf("%s/%s.bit", self.dir, self.name), os.O_CREATE|os.O_RDWR, 0666); err != nil {
		log.Fatal(err)
	}
	log.Println("open ", fmt.Sprintf("%s/%s.bit", self.dir, self.name))
	self.bits, err = ioutil.ReadAll(self.fp)
	if err != nil {
		log.Fatal(err)
	}
	self.numBits = len(self.bits)*8 - 1
	if len(self.bits) == 0 {
		self.nextBits = 0
		self.bits = make([]byte, 1)
	} else {
		for i := self.numBits; i >= 0; i-- {
			if !self.At(i) {
				self.nextBits = i + 1
				self.free.PushFront(i)
			}
		}
	}
}

func (self *KiteBitsetDisk) ensureCapacity(numBits int) {
	if numBits <= self.numBits {
		return
	}
	bitsLen := (numBits + 1) / 8
	if bitsLen%8 != 0 {
		bitsLen++
	}

	if len(self.bits)*2 > bitsLen {
		// 扩容一倍
		self.bits = append(self.bits, make([]byte, len(self.bits))...)
	} else {
		// 扩到指定容量
		self.bits = append(self.bits, make([]byte, bitsLen-len(self.bits))...)
	}
	self.numBits = len(self.bits) - 1
}

func (self *KiteBitsetDisk) At(index int) bool {
	self.ensureCapacity(index)
	return (self.bits[index/8] & (0x80 >> byte(index%8))) != 0
}

func (self *KiteBitsetDisk) Set(index int, status bool) {
	self.ensureCapacity(index)
	if status == true {
		self.bits[index/8] |= (0x80 >> byte(index%8))
	} else {
		self.bits[index/8] &= (0x80 >> byte(index%8)) ^ 0xf
		self.free.PushFront(index)
	}

	if self.nextBits < index {
		self.nextBits = index + 1
	}
	// self.fp.Seek(int64(index/8), 0)
	// self.fp.Write([]byte{self.bits[index/8]})
}

func (self *KiteBitsetDisk) Next() int {
	if self.free.Len() > 0 {
		e := self.free.Front()
		self.free.Remove(e)
		return e.Value.(int)
	}
	return self.nextBits
}

func (self *KiteBitsetDisk) Flush() {
	self.fp.Seek(0, 0)
	self.fp.Write(self.bits)
}
