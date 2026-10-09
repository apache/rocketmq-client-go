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
	"context"
	errors2 "github.com/apache/rocketmq-client-go/v2/errors"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestShutdownReleasesPendingRebalance(t *testing.T) {
	pc, err := NewPushConsumer(WithInstance(t.Name()), WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})))
	require.NoError(t, err)
	defer pc.Shutdown()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storage := NewMockOffsetStore(ctrl)
	pc.storage = storage
	storage.EXPECT().remove(gomock.Any()).AnyTimes()
	// Model an in-flight rebalance whose offset lookup completes after shutdown.
	entered, resume := make(chan struct{}), make(chan struct{})
	first := true
	storage.EXPECT().readWithException(gomock.Any(), gomock.Any()).DoAndReturn(func(_ *primitive.MessageQueue, _ readType) (int64, error) {
		if first {
			first = false
			close(entered)
			<-resume
		}
		return 0, nil
	}).AnyTimes()
	queues := make([]*primitive.MessageQueue, cap(pc.prCh)+1)
	for i := range queues {
		queues[i] = &primitive.MessageQueue{Topic: "test", BrokerName: "broker", QueueId: i}
	}
	exited := make(chan struct{})
	go func() { defer close(exited); pc.updateProcessQueueTable("test", queues) }()
	<-entered
	require.NoError(t, pc.Shutdown())
	close(resume)
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Error("rebalance remains blocked sending to prCh after shutdown")
	}
	// Drain the orphaned channel so the reproduction itself does not leak.
	for {
		select {
		case <-exited:
			pc.processQueueTable.Range(func(_, _ interface{}) bool { t.Error("shutdown retained a newly published queue"); return true })
			return
		case <-pc.prCh:
		}
	}
}

func TestShutdownReleasesPushConcurrencyWait(t *testing.T) {
	pc, err := NewPushConsumer(WithInstance(t.Name()), WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})))
	require.NoError(t, err)
	defer pc.Shutdown()
	pc.option.ConsumeMessageBatchMaxSize = 1
	pq := newProcessQueue(false)
	mq := &primitive.MessageQueue{Topic: "test"}
	pq.msgCh <- []*primitive.MessageExt{{Message: primitive.Message{Topic: "test"}}}
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	pc.crCh.Store("test", slots)
	exited := make(chan struct{})
	go func() { defer close(exited); pc.consumeMessageConcurrently(pq, mq) }()
	time.Sleep(30 * time.Millisecond)
	require.NoError(t, pc.Shutdown())
	pq.WithDropped(true)
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Error("consumer dispatcher remains blocked on concurrency slot after shutdown")
	}
	<-slots
	<-exited
}

func TestShutdownWakesPoll(t *testing.T) {
	pc, err := NewPullConsumer(WithInstance(t.Name()), WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})))
	require.NoError(t, err)
	defer pc.Shutdown()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		_, err := pc.Poll(ctx, time.Hour)
		require.Equal(t, errors2.ErrService, err)
	}()
	require.NoError(t, pc.Shutdown())
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Error("Poll retains caller until its timeout even after Shutdown")
	}
	cancel()
	<-exited
}

func TestPollBeforeAndAfterShutdown(t *testing.T) {
	pc, err := NewPullConsumer(WithInstance(t.Name()), WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})))
	require.NoError(t, err)
	defer pc.Shutdown()
	_, err = pc.Poll(context.Background(), time.Millisecond)
	require.Equal(t, ErrNoNewMsg, err)
	cr := &ConsumeRequest{processQueue: newProcessQueue(false), msgList: []*primitive.MessageExt{{}}}
	pc.consumeRequestCache <- cr
	result, err := pc.Poll(context.Background(), time.Second)
	require.NoError(t, err)
	require.Same(t, cr, result)
	pc.consumeRequestCache <- cr
	require.NoError(t, pc.Shutdown())
	_, err = pc.Poll(context.Background(), time.Hour)
	require.Equal(t, errors2.ErrService, err, "closed Poll must not return a cached request")
}

func TestRebalanceDispatchesWhileOpen(t *testing.T) {
	pc, err := NewPushConsumer(WithInstance(t.Name()), WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})))
	require.NoError(t, err)
	defer pc.Shutdown()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storage := NewMockOffsetStore(ctrl)
	pc.storage = storage
	mq := &primitive.MessageQueue{Topic: "test", BrokerName: "broker"}
	storage.EXPECT().remove(mq)
	storage.EXPECT().readWithException(mq, gomock.Any()).Return(int64(42), nil)
	require.True(t, pc.updateProcessQueueTable("test", []*primitive.MessageQueue{mq}))
	select {
	case pr := <-pc.prCh:
		require.Equal(t, int64(42), pr.nextOffset)
		require.Equal(t, *mq, *pr.mq)
		require.False(t, pr.pq.IsDroppd())
		pr.pq.WithDropped(true)
	case <-time.After(time.Second):
		t.Fatal("rebalance did not dispatch its queue")
	}
}
