package lfu

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/recallsong/gocache"
)

func loader(ctx context.Context, key string, extra ...any) (string, error) {
	return fmt.Sprintf("valueFor%s", key), nil
}

func getSimpleEvictedFunc[K comparable, V any](t *testing.T) func(K, V) {
	return func(key K, value V) {
		t.Logf("Key=%v Value=%v will be evicted.\n", key, value)
	}
}

func testSetCache(t *testing.T, c *Cache[string, string], numbers int) {
	for i := 0; i < numbers; i++ {
		key := fmt.Sprintf("Key-%d", i)
		value, err := loader(context.Background(), key)
		if err != nil {
			t.Error(err)
			return
		}
		c.Set(key, value)
	}
}

func testGetCache(t *testing.T, c *Cache[string, string], numbers int) {
	for i := 0; i < numbers; i++ {
		key := fmt.Sprintf("Key-%d", i)
		v, err := c.Get(context.Background(), key)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		expectedV, _ := loader(context.Background(), key)
		if v != expectedV {
			t.Errorf("Expected value is %v, not %v", expectedV, v)
		}
	}
}

func TestLFUGet(t *testing.T) {
	size := 1000

	cache := New(gocache.Config[string, string]{
		Size:        size,
		EvictedFunc: getSimpleEvictedFunc[string, string](t),
	})
	testSetCache(t, cache, size)
	testGetCache(t, cache, size)
}

func TestLoadingLFUGet(t *testing.T) {
	size := 1000
	cache := New(gocache.Config[string, string]{
		Size:        size,
		EvictedFunc: getSimpleEvictedFunc[string, string](t),
		LoaderFunc:  loader,
	})
	testGetCache(t, cache, size)
}

func TestLFULength(t *testing.T) {
	cache := New(gocache.Config[string, string]{
		Size:        1000,
		EvictedFunc: getSimpleEvictedFunc[string, string](t),
		LoaderFunc:  loader,
	})
	cache.Get(context.Background(), "test1")
	cache.Get(context.Background(), "test2")
	length := cache.Len()
	expectedLength := 2
	if length != expectedLength {
		t.Errorf("Expected length is %v, not %v", length, expectedLength)
	}
}

func TestLFUEvictItem(t *testing.T) {
	numbers := 11
	cache := New(gocache.Config[string, string]{
		Size:        10,
		EvictedFunc: getSimpleEvictedFunc[string, string](t),
		LoaderFunc:  loader,
	})

	for i := 0; i < numbers; i++ {
		_, err := cache.Get(context.Background(), fmt.Sprintf("Key-%d", i))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}
}

func TestLFUGetIFPresent(t *testing.T) {
	cache := New(gocache.Config[string, string]{
		Size:        8,
		EvictedFunc: getSimpleEvictedFunc[string, string](t),
		LoaderFunc: func(ctx context.Context, key string, extra ...any) (string, error) {
			return "value", nil
		},
	})

	_, err := cache.GetIFPresent("key")
	if err != gocache.ErrKeyNotFound {
		t.Errorf("err should not be %v", err)
	}

	time.Sleep(2 * time.Millisecond)

	v, err := cache.Get(context.Background(), "key")
	if err != nil {
		t.Errorf("err should not be %v", err)
	}
	if v != "value" {
		t.Errorf("v should not be %v", v)
	}
}

func TestLFUHas(t *testing.T) {
	cache := New(gocache.Config[string, string]{
		Size:        2,
		Expiration:  10 * time.Millisecond,
		EvictedFunc: getSimpleEvictedFunc[string, string](t),
		LoaderFunc:  loader,
	})

	for i := 0; i < 10; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			cache.Get(context.Background(), "test1")
			cache.Get(context.Background(), "test2")

			if cache.Has("test0") {
				t.Fatal("should not have test0")
			}
			if !cache.Has("test1") {
				t.Fatal("should have test1")
			}
			if !cache.Has("test2") {
				t.Fatal("should have test2")
			}

			time.Sleep(20 * time.Millisecond)

			if cache.Has("test0") {
				t.Fatal("should not have test0")
			}
			if cache.Has("test1") {
				t.Fatal("should not have test1")
			}
			if cache.Has("test2") {
				t.Fatal("should not have test2")
			}
		})
	}
}

func TestLFUFreqListOrder(t *testing.T) {
	cache := New(gocache.Config[int, int]{
		Size:        5,
		EvictedFunc: getSimpleEvictedFunc[int, int](t),
	})
	for i := 4; i >= 0; i-- {
		cache.Set(i, i)
		for j := 0; j <= i; j++ {
			cache.Get(context.Background(), i)
		}
	}
	if l := cache.freqList.Len(); l != 6 {
		t.Fatalf("%v != 6", l)
	}
	var i uint
	for e := cache.freqList.Front(); e != nil; e = e.Next() {
		if e.Value.(*freqEntry[int, int]).freq != i {
			t.Fatalf("%v != %v", e.Value.(*freqEntry[int, int]).freq, i)
		}
		i++
	}
	cache.Remove(1)

	if l := cache.freqList.Len(); l != 5 {
		t.Fatalf("%v != 5", l)
	}
	cache.Set(1, 1)
	if l := cache.freqList.Len(); l != 5 {
		t.Fatalf("%v != 5", l)
	}
	cache.Get(context.Background(), 1)
	if l := cache.freqList.Len(); l != 5 {
		t.Fatalf("%v != 5", l)
	}
	cache.Get(context.Background(), 1)
	if l := cache.freqList.Len(); l != 6 {
		t.Fatalf("%v != 6", l)
	}
}

func TestLFUFreqListLength(t *testing.T) {
	k0, v0 := "k0", "v0"
	k1, v1 := "k1", "v1"

	{
		cache := New(gocache.Config[string, string]{
			Size:        5,
			EvictedFunc: getSimpleEvictedFunc[string, string](t),
		})
		if l := cache.freqList.Len(); l != 1 {
			t.Fatalf("%v != 1", l)
		}
	}
	{
		cache := New(gocache.Config[string, string]{
			Size:        5,
			EvictedFunc: getSimpleEvictedFunc[string, string](t),
		})
		cache.Set(k0, v0)
		for i := 0; i < 5; i++ {
			cache.Get(context.Background(), k0)
		}
		if l := cache.freqList.Len(); l != 2 {
			t.Fatalf("%v != 2", l)
		}
	}

	{
		cache := New(gocache.Config[string, string]{
			Size:        5,
			EvictedFunc: getSimpleEvictedFunc[string, string](t),
		})
		cache.Set(k0, v0)
		cache.Set(k1, v1)
		for i := 0; i < 5; i++ {
			cache.Get(context.Background(), k0)
			cache.Get(context.Background(), k1)
		}
		if l := cache.freqList.Len(); l != 2 {
			t.Fatalf("%v != 2", l)
		}
	}

	{
		cache := New(gocache.Config[string, string]{
			Size:        5,
			EvictedFunc: getSimpleEvictedFunc[string, string](t),
		})
		cache.Set(k0, v0)
		cache.Set(k1, v1)
		for i := 0; i < 5; i++ {
			cache.Get(context.Background(), k0)
		}
		if l := cache.freqList.Len(); l != 2 {
			t.Fatalf("%v != 2", l)
		}
		for i := 0; i < 5; i++ {
			cache.Get(context.Background(), k1)
		}
		if l := cache.freqList.Len(); l != 2 {
			t.Fatalf("%v != 2", l)
		}
	}

	{
		cache := New(gocache.Config[string, string]{
			Size:        5,
			EvictedFunc: getSimpleEvictedFunc[string, string](t),
		})
		cache.Set(k0, v0)
		cache.Get(context.Background(), k0)
		if l := cache.freqList.Len(); l != 2 {
			t.Fatalf("%v != 2", l)
		}
		cache.Remove(k0)
		if l := cache.freqList.Len(); l != 1 {
			t.Fatalf("%v != 1", l)
		}
		cache.Set(k0, v0)
		if l := cache.freqList.Len(); l != 1 {
			t.Fatalf("%v != 1", l)
		}
		cache.Get(context.Background(), k0)
		if l := cache.freqList.Len(); l != 2 {
			t.Fatalf("%v != 2", l)
		}
	}
}
