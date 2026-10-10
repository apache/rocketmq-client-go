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
	"bytes"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/stretchr/testify/require"
)

func TestTransactionWorkerStopsOnShutdown(t *testing.T) {
	// Keep another owner alive: stopping this worker must not require disposing
	// the shared RMQ client used by the remaining producer.
	other, err := NewDefaultProducer(WithInstanceName(t.Name()), WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})))
	require.NoError(t, err)
	defer other.Shutdown()

	tp, err := NewTransactionProducer(nil,
		WithInstanceName(t.Name()),
		WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})))
	require.NoError(t, err)
	defer tp.Shutdown()

	// Observe the actual worker's exit without relying on process-wide
	// goroutine counts or sleeping until an unrelated SDK worker exits.
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		tp.checkTransactionState()
	}()
	require.NoError(t, tp.Shutdown())
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("transaction worker remained blocked after shutdown")
	}
	require.Error(t, tp.Start())
}

func transactionWorkerCount() int {
	var b bytes.Buffer
	pprof.Lookup("goroutine").WriteTo(&b, 2)
	return strings.Count(b.String(), "(*transactionProducer).checkTransactionState(")
}

func TestTransactionRepeatedLifecycle(t *testing.T) {
	baseline := transactionWorkerCount()
	for i := 0; i < 10; i++ {
		opts := []Option{WithInstanceName(t.Name()), WithGroupName(t.Name()), WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}))}
		p, err := NewTransactionProducer(nil, opts...)
		require.NoError(t, err)
		defer p.Shutdown()
		for j := 0; j < 3; j++ {
			require.NoError(t, p.Start())
		}
		require.Eventually(t, func() bool { return transactionWorkerCount() == baseline+1 }, time.Second, time.Millisecond)
		failed, err := NewTransactionProducer(nil, opts...)
		require.NoError(t, err)
		defer failed.Shutdown()
		require.Error(t, failed.Start())
		require.Error(t, failed.Start(), "repeated failed Start must not launch a transaction worker")
		require.NoError(t, failed.Shutdown())
		require.NoError(t, p.Shutdown())
		require.Eventually(t, func() bool { return transactionWorkerCount() <= baseline }, time.Second, time.Millisecond)
	}
}
