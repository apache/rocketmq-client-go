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
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRingRead(t *testing.T) {
	rb := NewRingNodesBuffer(5)
	assert.Equal(t, uint64(8), rb.Cap())

	err := rb.Write([]byte("hello"))
	if !assert.Nil(t, err) {
		return
	}
	data, err := rb.Read(1 * time.Second)
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, "hello", string(data))
}

func TestRingReadBySize(t *testing.T) {
	rb := NewRingNodesBuffer(5)
	assert.Equal(t, uint64(8), rb.Cap())

	err := rb.Write([]byte("hello"))
	if !assert.Nil(t, err) {
		return
	}
	sink := make([]byte, 5)
	n, err := rb.ReadBySize(sink, 1*time.Second)
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", string(sink))
}

func BenchmarkRingReadBufferMPMC(b *testing.B) {
	q := NewRingNodesBuffer(uint64(b.N * 100))
	var wg sync.WaitGroup
	wg.Add(100)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < 100; i++ {
		go func() {
			for i := 0; i < b.N; i++ {
				q.Write([]byte(strconv.Itoa(i)))
			}
		}()
	}

	for i := 0; i < 100; i++ {
		go func() {
			for i := 0; i < b.N; i++ {
				_ = len(strconv.Itoa(i))
				var p []byte
				p, _ = q.Read(1 * time.Second)
				fmt.Sprintf("%v", p)

			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func BenchmarkRingBySizeBufferMPMC(b *testing.B) {
	q := NewRingNodesBuffer(uint64(b.N * 100))
	var wg sync.WaitGroup
	wg.Add(100)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < 100; i++ {
		go func() {
			for i := 0; i < b.N; i++ {
				q.Write([]byte(strconv.Itoa(i)))
			}
		}()
	}

	for i := 0; i < 100; i++ {
		go func() {
			for i := 0; i < b.N; i++ {
				p := make([]byte, len(strconv.Itoa(i)))
				q.ReadBySize(p, 1*time.Second)
			}
			wg.Done()
		}()
	}

	wg.Wait()
}
