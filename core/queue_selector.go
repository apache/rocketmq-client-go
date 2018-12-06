package rocketmq

import "C"
import (
	"strconv"
	"sync"
	"unsafe"
)

var selectors = selectorHolder{selectors: map[int]*messageQueueSelectorWrapper{}}

//export queueSelectorCallback
func queueSelectorCallback(size int, selectorKey unsafe.Pointer) int {
	s, ok := selectors.getAndDelete(*(*int)(selectorKey))
	if !ok {
		panic("BUG: not register the selector with key:" + strconv.Itoa(*(*int)(selectorKey)))
	}
	return s.Select(size)
}

type messageQueueSelectorWrapper struct {
	selector MessageQueueSelector

	m   *Message
	arg interface{}
}

func (w *messageQueueSelectorWrapper) Select(size int) int {
	return w.selector.Select(size, w.m, w.arg)
}

// MessageQueueSelector select one message queue
type MessageQueueSelector interface {
	Select(size int, m *Message, arg interface{}) int
}

type selectorHolder struct {
	sync.Mutex

	selectors map[int]*messageQueueSelectorWrapper
	key       int
}

func (s *selectorHolder) put(selector *messageQueueSelectorWrapper) (key int) {
	s.Lock()
	key = s.key
	s.selectors[key] = selector
	s.key++
	s.Unlock()
	return
}

func (s *selectorHolder) getAndDelete(key int) (*messageQueueSelectorWrapper, bool) {
	s.Lock()
	selector, ok := s.selectors[key]
	if ok {
		delete(s.selectors, key)
	}
	s.Unlock()

	return selector, ok
}
