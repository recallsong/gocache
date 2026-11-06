package lru

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/recallsong/gocache/common"
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

func TestLRUGet(t *testing.T) {
	size := 1000

	cache := New(Config[string, string]{
		Size:        size,
		EvictedFunc: getSimpleEvictedFunc[string, string](t),
	})
	testSetCache(t, cache, size)
	testGetCache(t, cache, size)
}

func TestLoadingLRUGet(t *testing.T) {
	size := 1000
	cache := New(Config[string, string]{
		Size:        size,
		EvictedFunc: getSimpleEvictedFunc[string, string](t),
		LoaderFunc:  loader,
	})
	testGetCache(t, cache, size)
}

func TestLRULength(t *testing.T) {
	cache := New(Config[string, string]{
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

func TestLRUEvictItem(t *testing.T) {
	numbers := 11
	cache := New(Config[string, string]{
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

func TestLRUGetIFPresent(t *testing.T) {
	cache := New(Config[string, string]{
		Size:        8,
		EvictedFunc: getSimpleEvictedFunc[string, string](t),
		LoaderFunc: func(ctx context.Context, key string, extra ...any) (string, error) {
			return "value", nil
		},
	})

	_, err := cache.GetIFPresent("key")
	if err != common.ErrKeyNotFound {
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

func TestLRUHas(t *testing.T) {
	cache := New(Config[string, string]{
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
