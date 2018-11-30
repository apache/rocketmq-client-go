package rocketmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockMessageQueueSelector struct {
	arg  interface{}
	m    *Message
	size int

	selectRet int
}

func (m *mockMessageQueueSelector) Select(size int, msg *Message, arg interface{}) int {
	m.arg, m.m, m.size = arg, msg, size
	return m.selectRet
}

func TestWraper(t *testing.T) {
	s := &mockMessageQueueSelector{selectRet: 2}
	w := &messageQueueSelectorWrapper{selector: s, m: &Message{}, arg: 3}

	assert.Equal(t, 2, w.Select(4))
	assert.Equal(t, w.m, s.m)
	v, ok := s.arg.(int)
	assert.True(t, ok)
	assert.Equal(t, 3, v)
}

func TestSelectorHolder(t *testing.T) {
	s := &messageQueueSelectorWrapper{}

	key := selectors.put(s)
	assert.Equal(t, 0, key)

	key = selectors.put(s)
	assert.Equal(t, 1, key)

	assert.Equal(t, 2, len(selectors.selectors))

	ss, ok := selectors.getAndDelete(0)
	assert.Equal(t, s, ss)
	assert.True(t, ok)

	ss, ok = selectors.getAndDelete(1)
	assert.Equal(t, s, ss)
	assert.True(t, ok)

	assert.Equal(t, 0, len(selectors.selectors))
}
