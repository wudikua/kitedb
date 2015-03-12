package util

import (
	"log"
	"testing"
)

func TestBitset(t *testing.T) {
	bitset := NewKiteBitsetDisk("bitset", "test")

	for i := 0; i < 7; i++ {
		bitset.Set(bitset.Next(), true)
	}

	bitset.Flush()
	log.Println("flush end")
	// bitset = NewKiteBitsetDisk("bitset", "test")
	// log.Println("next:", bitset.Next())
	// bitset.Set(bitset.Next(), true)
}
