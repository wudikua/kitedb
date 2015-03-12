package util

import (
	"encoding/binary"
	"errors"
	"github.com/xuyu/goredis"
	"log"
)

type Type interface{}

// 一个固定长度的环状队列
type KiteRingQueue struct {
	front int // 队头指针
	rear  int // 队尾指针
	size  int // 队列最大长度
	data  []Type
}

func (self *KiteRingQueue) Len() int {
	return (self.front - self.rear + self.size) % self.size
}

func NewKiteRingQueue(size int) *KiteRingQueue {
	return &KiteRingQueue{
		size:  size,
		front: 0,
		rear:  0,
		data:  make([]Type, size+1),
	}
}

func (self *KiteRingQueue) Enqueue(e Type) error {
	//牺牲一个存储单元判断队列为满
	if (self.rear+1)%self.size == self.front {
		return errors.New("queue is full")
	}
	self.data[self.rear] = e
	self.rear = (self.rear + 1) % self.size
	return nil
}

func (self *KiteRingQueue) Dequeue() (Type, error) {
	if self.rear == self.front {
		return nil, errors.New("queue is empty")
	}
	data := self.data[self.front]
	self.front = (self.front + 1) % self.size
	return data, nil
}

type KiteBitsetRedis struct {
	numBits int
	bits    []byte
}

func NewKiteBitset() *KiteBitsetRedis {
	redis, err := goredis.Dial(&goredis.DialConfig{Address: ":6379"})
	if err != nil {
		log.Fatal("page bits redis down")
	}
	r, _ := redis.ExecuteCommand("GET", "page_bits")
	bits, _ := r.BytesValue()
	r, _ = redis.ExecuteCommand("GET", "page_bits_num")
	num_bytes, _ := r.BytesValue()
	var b *KiteBitsetRedis
	if bits != nil && len(num_bytes) > 0 {
		num := int(binary.BigEndian.Uint32(num_bytes))
		b = &KiteBitsetRedis{numBits: num, bits: bits}
	} else {
		b = &KiteBitsetRedis{numBits: 0, bits: make([]byte, 0)}
	}
	return b
}

func (b *KiteBitsetRedis) Flush() {
	clone := &KiteBitsetRedis{numBits: b.numBits, bits: b.bits[:]}
	redis, err := goredis.Dial(&goredis.DialConfig{Address: ":6379"})
	if err != nil {
		log.Fatal("page bits redis down")
	}
	redis.ExecuteCommand("SET", "page_bits", clone.bits)
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(clone.numBits))
	redis.ExecuteCommand("SET", "page_bits_num", bs)
}

func (b *KiteBitsetRedis) AppendBytes(data []byte) {
	for _, d := range data {
		b.AppendByte(d, 8)
	}
}

func (b *KiteBitsetRedis) AppendByte(value byte, numBits int) {
	b.ensureCapacity(numBits)

	if numBits > 8 {
		log.Fatal("numBits %d out of range 0-8", numBits)
	}

	for i := numBits - 1; i >= 0; i-- {
		if value&(1<<uint(i)) != 0 {
			b.bits[b.numBits/8] |= 0x80 >> uint(b.numBits%8)
		}

		b.numBits++
	}
}

func (b *KiteBitsetRedis) ensureCapacity(numBits int) {
	numBits += b.numBits

	newNumBytes := numBits / 8
	if numBits%8 != 0 {
		newNumBytes++
	}

	if len(b.bits) >= newNumBytes {
		return
	}

	b.bits = append(b.bits, make([]byte, newNumBytes+2*len(b.bits))...)
}

func (b *KiteBitsetRedis) Len() int {
	return b.numBits
}

func (b *KiteBitsetRedis) Next() int {
	return b.numBits
}

func (b *KiteBitsetRedis) At(index int) bool {
	b.ensureCapacity(index)

	return (b.bits[index/8] & (0x80 >> byte(index%8))) != 0
}

func (b *KiteBitsetRedis) Set(index int, status bool) {
	b.ensureCapacity(index)

	if status == true {
		b.bits[index/8] |= (0x80 >> byte(index%8))
	} else {
		b.bits[index/8] &= (0x80 >> byte(index%8)) ^ 0xf
	}
}
