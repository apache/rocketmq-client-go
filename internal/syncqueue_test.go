package internal

import (
	"testing"
	"time"
)

func TestSyncQueue(t *testing.T) {
	ch := NewSyncQueue(10)
	for i := 1; i <= 20; i++ {
		go func(n int) {
			t.Log(ch.Pop(), n)
		}(i)
	}
	for i := 1; i <= 10; i++ {
		ch.Push(i)
		time.Sleep(100 * time.Millisecond)
	}
	ch.Close()
}

func TestBuffer(t *testing.T) {
	ch := NewSyncQueue(1000)
	for i := 1; i <= 513; i++ {
		ch.Push(i)
	}
	t.Log(len(ch.buffer.buf), ch.Len())
	for i := 1; i <= 500; i++ {
		ch.Pop()
	}
	t.Log(len(ch.buffer.buf), ch.Len())
}
