/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package utils

import (
	"runtime"
	"sync/atomic"
	"time"
)

// 1.需要能够动态扩容
// 2.缩容看情况
// 3.read的时候需要block
// 4.线程安全
type RingNodesBuffer struct {
	writePos uint64
	readPos  uint64
	mask     uint64

	nodes nodes
}

type node struct {
	position uint64
	buf      []byte
}

type nodes []*node

// roundUp takes a uint64 greater than 0 and rounds it up to the next
// power of 2.
func roundUp(v uint64) uint64 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}

func (rb *RingNodesBuffer) init(size uint64) {
	size = roundUp(size)
	rb.nodes = make(nodes, size)
	for i := uint64(0); i < size; i++ {
		rb.nodes[i] = &node{position: i}
	}
	rb.mask = size - 1 // so we don't have to do this with every put/get operation
}

func NewRingNodesBuffer(cap uint64) *RingNodesBuffer {
	rb := &RingNodesBuffer{}
	rb.init(cap)
	//go rb.resize()
	return rb
}

func (r *RingNodesBuffer) Write(b []byte) error {
	var n *node
	var dif uint64
	pos := atomic.LoadUint64(&r.writePos)
	i := 0
L:
	for {
		// pos 16 seq 1.     0001 0000    00001111   0001 1111
		n = r.nodes[pos&r.mask]
		seq := atomic.LoadUint64(&n.position)
		switch dif = seq - pos; {
		case dif == 0:
			if atomic.CompareAndSwapUint64(&r.writePos, pos, pos+1) {
				break L
			}
		default:
			pos = atomic.LoadUint64(&r.writePos)
		}
		if i == 10000 {
			runtime.Gosched() // free up the cpu before the next iteration
			i = 0
		} else {
			i++
		}
	}

	n.buf = b
	atomic.StoreUint64(&n.position, pos+1)
	return nil
}

// 直接返回数据
func (r *RingNodesBuffer) Read(timeout time.Duration) (data []byte, err error) {
	var (
		node  *node
		pos   = atomic.LoadUint64(&r.readPos)
		start time.Time
		dif   uint64
	)
	if timeout > 0 {
		start = time.Now()
	}
	i := 0
L:
	for {
		node = r.nodes[pos&r.mask]
		seq := atomic.LoadUint64(&node.position)
		switch dif = seq - (pos + 1); {
		case dif == 0:
			if atomic.CompareAndSwapUint64(&r.readPos, pos, pos+1) {
				break L
			}
		default:
			pos = atomic.LoadUint64(&r.readPos)
		}
		if timeout > 0 && time.Since(start) >= timeout {
			return
		}
		if i == 10000 {
			runtime.Gosched() // free up the cpu before the next iteration
			i = 0
		} else {
			i++
		}
	}
	data = node.buf
	atomic.StoreUint64(&node.position, pos+r.mask+1)
	return
}

// 知道大小，传进去解析
func (r *RingNodesBuffer) ReadBySize(data []byte, timeout time.Duration) (n int, err error) {
	var (
		node  *node
		pos   = atomic.LoadUint64(&r.readPos)
		start time.Time
		dif   uint64
	)
	i := 0
	if timeout > 0 {
		start = time.Now()
	}
L:
	for {
		node = r.nodes[pos&r.mask]
		seq := atomic.LoadUint64(&node.position)
		switch dif = seq - (pos + 1); {
		case dif == 0:
			if atomic.CompareAndSwapUint64(&r.readPos, pos, pos+1) {
				break L
			}
		default:
			pos = atomic.LoadUint64(&r.readPos)
		}
		if timeout > 0 && time.Since(start) >= timeout {
			return
		}
		if i == 10000 {
			runtime.Gosched() // free up the cpu before the next iteration
			i = 0
		} else {
			i++
		}
	}
	n = copy(data, node.buf)
	atomic.StoreUint64(&node.position, pos+r.mask+1)
	return
}

func (r *RingNodesBuffer) Size() uint64 {
	return atomic.LoadUint64(&r.writePos) - atomic.LoadUint64(&r.readPos)

}

// Cap returns the capacity of this ring buffer.
func (rb *RingNodesBuffer) Cap() uint64 {
	return uint64(len(rb.nodes))
}

func (r *RingNodesBuffer) Destroy() {

}

func (r *RingNodesBuffer) resize() {
	// TODO
}
