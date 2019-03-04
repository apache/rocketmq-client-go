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

import "sync"

type RingBuffer struct {
	buf      []byte
	writePos int
	readPos  int
	cap      int
	rwMutex  sync.RWMutex
	exitCh   chan interface{}
}

func NewRingRBuffer(cap int) *RingBuffer {
	rb := &RingBuffer{buf: make([]byte, cap), cap: cap}
	go rb.resize()
	return rb
}

func (r *RingBuffer) Write(b []byte) error {
	// TODO
	return nil
}

func (r *RingBuffer) Read(p []byte) (n int, err error) {

	if r.Size() >= len(p) {
		copy(p, r.buf[r.readPos:r.readPos+len(p)])
		r.readPos += len(p)

	}

	// TODO waiting data...
	return 0, err
}

func (r *RingBuffer) Size() int {
	return r.writePos - r.readPos
}

func (r *RingBuffer) Destroy() {

}

func (r *RingBuffer) resize() {
	// TODO
}
