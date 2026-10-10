package consumer

import "sync"

// Sets 集合
// pushConsumer.subscribedTopic 原本使用的未加锁的 map[string]string，会有并发问题（concurrent map writes）
// 且原本的使用中只用了 key字段，这里单独写了一个 Sets 实现，替换 pushConsumer.subscribedTopic 解决 concurrent map writes 问题
type Sets struct {
	lock *sync.RWMutex
	m    map[interface{}]struct{}
}

func NewSets() *Sets {
	return &Sets{
		lock: new(sync.RWMutex),
		m:    make(map[interface{}]struct{}),
	}
}

func (m *Sets) Len() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return len(m.m)
}

func (m *Sets) Set(key interface{}) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.m[key] = struct{}{}
}

func (m *Sets) Exist(key interface{}) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	_, ok := m.m[key]
	return ok
}

func (m *Sets) Each(f func(k interface{}) bool) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for k := range m.m {
		if !f(k) {
			return false
		}
	}
	return true
}
