package util

import (
	"log"
	"testing"
)

func TestBitset(t *testing.T) {
	bitset := NewKiteBitsetDisk("bitset", "test")
	log.Println("next:", bitset.Next())
	bitset.Set(bitset.Next(), true)
	bitset.Flush()
	// bitset = NewKiteBitsetDisk("bitset", "test")
	// log.Println("next:", bitset.Next())
	// bitset.Set(bitset.Next(), true)
}
