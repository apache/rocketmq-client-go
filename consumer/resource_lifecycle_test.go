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

package consumer

import (
	"bytes"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/stretchr/testify/require"
)

func consumerBackgroundWorkers() int {
	var b bytes.Buffer
	pprof.Lookup("goroutine").WriteTo(&b, 2)
	return strings.Count(b.String(), "(*pushConsumer).Start.func") + strings.Count(b.String(), "(*statsItemSet).init.func")
}

func TestConsumerRepeatedLifecycleStopsBackgroundWorkers(t *testing.T) {
	baseline := consumerBackgroundWorkers()
	for i := 0; i < 10; i++ {
		pc, err := NewPushConsumer(WithInstance(t.Name()), WithGroupName(t.Name()), WithConsumerModel(BroadCasting), WithConsumeTimeout(15*time.Minute), WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})))
		require.NoError(t, err)
		defer pc.Shutdown()
		require.NoError(t, pc.Start())
		require.NoError(t, pc.Shutdown())
		require.Eventually(t, func() bool { return consumerBackgroundWorkers() <= baseline }, time.Second, time.Millisecond, "shutdown left cleanup/statistics workers waiting")
	}
}

func TestShutdownInterruptsConsumerRetryDelay(t *testing.T) {
	pc := &pushConsumer{done: make(chan struct{})}
	exited := make(chan struct{})
	go func() { pc.tryLockLaterAndReconsume(nil, 15*60*1000); close(exited) }()
	close(pc.done)
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("retry delay ignored shutdown")
	}
}
