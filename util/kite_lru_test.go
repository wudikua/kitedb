package util

import (
	"testing"
)

func TestLRU(t *testing.T) {
	cache := NewKiteLRUCache(3)
	cache.Set(1, 1)
	cache.Set(2, 2)
	cache.Set(3, 3)
	cache.Set(4, 4)
	_, contains := cache.Get(1)
	if contains {
		t.Fail()
	}
	cache.Delete(4)
	_, contains = cache.Get(4)
	if contains {
		t.Fail()
	}
}
