/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package producer

import (
	"testing"

	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/stretchr/testify/assert"
)

func TestRoundRobin(t *testing.T) {
	queues := 10
	s := NewRoundRobinQueueSelector()

	m := &primitive.Message{
		Topic: "test",
	}
	mrr := &primitive.Message{
		Topic: "rr",
	}
	for i := 0; i < 100; i++ {
		q := s.Select(m, queues)
		assert.Equal(t, (i+1)%queues, q, "i: %d", i)

		qrr := s.Select(mrr, queues)
		assert.Equal(t, (i+1)%queues, qrr, "i: %d", i)
	}
}
