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
	"errors"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestRebalanceInitializesFlowControlBeforeDispatch(t *testing.T) {
	pc, err := NewPushConsumer(WithInstance(t.Name()), WithGroupName(t.Name()),
		WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		WithPullThresholdForTopic(5000), WithPullThresholdSizeForTopic(64))
	require.NoError(t, err)
	defer pc.Shutdown()
	require.NoError(t, pc.validate())
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storage := NewMockOffsetStore(ctrl)
	pc.storage = storage
	storage.EXPECT().remove(gomock.Any()).Times(2)
	entered, resume := make(chan struct{}), make(chan struct{})
	calls := 0
	storage.EXPECT().readWithException(gomock.Any(), _ReadFromStore).DoAndReturn(
		func(_ *primitive.MessageQueue, _ readType) (int64, error) {
			calls++
			if calls == 2 {
				close(entered)
				<-resume
			}
			return 42, nil
		}).Times(2)
	queues := []*primitive.MessageQueue{
		{Topic: "test", BrokerName: "broker", QueueId: 0},
		{Topic: "test", BrokerName: "broker", QueueId: 1},
	}
	done := make(chan bool, 1)
	go func() { done <- pc.updateProcessQueueTable("test", queues) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(resume)
		t.Fatal("second offset lookup did not start")
	}
	// The second lookup is blocked, independent of map iteration order.
	select {
	case <-pc.prCh:
		t.Error("pull dispatched before queue initialization completed")
	default:
	}
	close(resume)
	select {
	case pr := <-pc.prCh:
		require.Equal(t, int64(2500), pc.option.PullThresholdForQueue.Load())
		require.Equal(t, int32(32), pc.option.PullThresholdSizeForQueue.Load())
		require.Equal(t, int64(42), pr.nextOffset)
	case <-time.After(time.Second):
		t.Fatal("no pull dispatched after initialization")
	}
	select {
	case changed := <-done:
		require.True(t, changed)
	case <-time.After(time.Second):
		t.Fatal("rebalance did not finish")
	}
}

// No broker or background pull loop is started by this fixture.
func newFlowControlTestConsumer(t *testing.T) (*pushConsumer, *MockOffsetStore, func()) {
	t.Helper()
	pc, err := NewPushConsumer(WithInstance(t.Name()), WithGroupName("flow-control-test"),
		WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		WithPullThresholdForTopic(5000), WithPullThresholdSizeForTopic(64))
	require.NoError(t, err)
	require.NoError(t, pc.validate())
	ctrl := gomock.NewController(t)
	storage := NewMockOffsetStore(ctrl)
	pc.storage = storage
	storage.EXPECT().remove(gomock.Any()).AnyTimes()
	storage.EXPECT().persist(gomock.Any()).AnyTimes()
	return pc, storage, func() { pc.Shutdown(); ctrl.Finish() }
}

func flowControlTestQueues(n int) []*primitive.MessageQueue {
	queues := make([]*primitive.MessageQueue, n)
	for i := range queues {
		queues[i] = &primitive.MessageQueue{Topic: "test", BrokerName: "broker", QueueId: i}
	}
	return queues
}

func TestRebalanceFlowControlTracksQueueChanges(t *testing.T) {
	pc, storage, cleanup := newFlowControlTestConsumer(t)
	defer cleanup()
	storage.EXPECT().readWithException(gomock.Any(), _ReadFromStore).Return(int64(42), nil).Times(4)
	queues := flowControlTestQueues(4)
	for _, n := range []int{2, 4, 1, 0} {
		require.True(t, pc.updateProcessQueueTable("test", queues[:n]))
		if n > 0 {
			require.Equal(t, int64(5000/n), pc.option.PullThresholdForQueue.Load())
			require.Equal(t, int32(64/n), pc.option.PullThresholdSizeForQueue.Load())
		}
		expectedNew := 0
		if n == 2 || n == 4 {
			expectedNew = 2
		}
		require.Equal(t, expectedNew, len(pc.prCh))
		for len(pc.prCh) > 0 {
			<-pc.prCh
		}
		require.False(t, pc.updateProcessQueueTable("test", queues[:n]))
		require.Empty(t, pc.prCh, "unchanged rebalance must not dispatch again")
	}
	// An empty table leaves the last valid thresholds in place.
	require.Equal(t, int32(64), pc.option.PullThresholdSizeForQueue.Load())
}

func TestRebalanceFlowControlIgnoresFailedOffsetLookup(t *testing.T) {
	pc, storage, cleanup := newFlowControlTestConsumer(t)
	defer cleanup()
	queues := flowControlTestQueues(3)
	storage.EXPECT().readWithException(queues[0], _ReadFromStore).Return(int64(-1), errors.New("offset unavailable"))
	storage.EXPECT().readWithException(queues[1], _ReadFromStore).Return(int64(42), nil)
	storage.EXPECT().readWithException(queues[2], _ReadFromStore).Return(int64(42), nil)
	require.True(t, pc.updateProcessQueueTable("test", queues))
	require.Equal(t, int32(32), pc.option.PullThresholdSizeForQueue.Load())
	require.Equal(t, int64(2500), pc.option.PullThresholdForQueue.Load())
	require.Equal(t, 2, len(pc.prCh))
	_, found := pc.processQueueTable.Load(*queues[0])
	require.False(t, found)
}

func TestRebalanceShutdownDisposesPendingQueues(t *testing.T) {
	for _, duringLookup := range []bool{true, false} {
		name := "dispatch"
		if duringLookup {
			name = "lookup"
		}
		t.Run(name, func(t *testing.T) {
			pc, storage, cleanup := newFlowControlTestConsumer(t)
			defer cleanup()
			// No receiver: dispatch must remain interruptible by shutdown.
			pc.prCh = make(chan PullRequest)
			entered, resume := make(chan struct{}), make(chan struct{})
			calls := 0
			storage.EXPECT().readWithException(gomock.Any(), _ReadFromStore).DoAndReturn(
				func(_ *primitive.MessageQueue, _ readType) (int64, error) {
					calls++
					if duringLookup && calls == 2 {
						close(entered)
						<-resume
					}
					return 42, nil
				}).AnyTimes()
			if !duringLookup {
				update := pc.defaultConsumer.updateQueueFlowControl
				pc.defaultConsumer.updateQueueFlowControl = func() { update(); close(entered) }
			}
			done := make(chan struct{})
			go func() { defer close(done); pc.updateProcessQueueTable("test", flowControlTestQueues(3)) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				close(resume)
				t.Fatal("rebalance did not reach shutdown checkpoint")
			}
			var pending []*processQueue
			pc.processQueueTable.Range(func(_, value interface{}) bool {
				pending = append(pending, value.(*processQueue))
				return true
			})
			require.NotEmpty(t, pending)
			require.NoError(t, pc.Shutdown())
			close(resume)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("shutdown did not release rebalance")
			}
			for _, pq := range pending {
				require.True(t, pq.IsDroppd())
			}
			pc.processQueueTable.Range(func(_, _ interface{}) bool { t.Error("pending queue retained after shutdown"); return true })
		})
	}
}

func TestRebalancePendingCleanupPreservesReplacement(t *testing.T) {
	pc, storage, cleanup := newFlowControlTestConsumer(t)
	defer cleanup()
	storage.EXPECT().readWithException(gomock.Any(), _ReadFromStore).Return(int64(42), nil)
	mq := flowControlTestQueues(1)[0]
	replacement := newProcessQueue(false)
	var pending *processQueue
	pc.defaultConsumer.updateQueueFlowControl = func() {
		value, _ := pc.processQueueTable.Load(*mq)
		pending = value.(*processQueue)
		pc.processQueueTable.Store(*mq, replacement)
		pc.Shutdown()
	}
	pc.updateProcessQueueTable("test", []*primitive.MessageQueue{mq})
	value, ok := pc.processQueueTable.Load(*mq)
	require.True(t, ok)
	require.Same(t, replacement, value)
	require.True(t, pending.IsDroppd())
	require.False(t, replacement.IsDroppd())
}

func TestRebalanceFlowControlPreservesThresholdSemantics(t *testing.T) {
	pc, _, cleanup := newFlowControlTestConsumer(t)
	defer cleanup()
	for _, mq := range flowControlTestQueues(65) {
		pc.processQueueTable.Store(*mq, newProcessQueue(false))
	}
	other := primitive.MessageQueue{Topic: "other", BrokerName: "broker"}
	pc.processQueueTable.Store(other, newProcessQueue(false))
	pc.updateQueueFlowControl()
	require.Equal(t, int32(1), pc.option.PullThresholdSizeForQueue.Load())
	require.Equal(t, int64(5000/66), pc.option.PullThresholdForQueue.Load())
	pc.option.PullThresholdForTopic = -1
	pc.option.PullThresholdSizeForTopic = -1
	pc.option.PullThresholdForQueue.Store(123)
	pc.option.PullThresholdSizeForQueue.Store(7)
	pc.updateQueueFlowControl()
	require.Equal(t, int64(123), pc.option.PullThresholdForQueue.Load())
	require.Equal(t, int32(7), pc.option.PullThresholdSizeForQueue.Load())
}

func TestRebalanceFlowControlSkipsUnlockedOrderlyQueue(t *testing.T) {
	pc, storage, cleanup := newFlowControlTestConsumer(t)
	defer cleanup()
	realClient := pc.client
	defer func() { pc.client = realClient }()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client, namesrv := internal.NewMockRMQClient(ctrl), internal.NewMockNamesrvs(ctrl)
	pc.client = client
	pc.consumeOrderly = true
	queues := flowControlTestQueues(2)
	queues[1].BrokerName = "unavailable"
	client.SetNameSrv(namesrv)
	namesrv.EXPECT().FindBrokerAddressInSubscribe("unavailable", internal.MasterId, true).Return(nil)
	namesrv.EXPECT().FindBrokerAddressInSubscribe("broker", internal.MasterId, true).Return(&internal.FindBrokerResult{BrokerAddr: "broker"})
	client.EXPECT().ClientID().Return("test-client")
	client.EXPECT().InvokeSync(gomock.Any(), "broker", gomock.Any(), gomock.Any()).Return(&remote.RemotingCommand{Body: []byte(`{"lockOKMQSet":[{"topic":"test","brokerName":"broker","queueId":0}]}`)}, nil)
	storage.EXPECT().readWithException(queues[0], _ReadFromStore).Return(int64(42), nil)
	require.True(t, pc.updateProcessQueueTable("test", queues))
	require.Equal(t, int64(5000), pc.option.PullThresholdForQueue.Load())
	require.Equal(t, int32(64), pc.option.PullThresholdSizeForQueue.Load())
	require.Equal(t, 1, len(pc.prCh))
}

// Exercise both doBalance call sites, including their notification callbacks.
func TestRebalanceFlowControlBeforeDispatchAcrossModels(t *testing.T) {
	for _, model := range []MessageModel{BroadCasting, Clustering} {
		t.Run(model.String(), func(t *testing.T) {
			pc, storage, cleanup := newFlowControlTestConsumer(t)
			defer cleanup()
			realClient := pc.client
			defer func() { pc.client = realClient }()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			client := internal.NewMockRMQClient(ctrl)
			pc.client = client
			pc.model = model
			queues := flowControlTestQueues(2)
			pc.topicSubscribeInfoTable.Store("test", queues)
			pc.subscriptionDataTable.Store("test", &internal.SubscriptionData{})
			storage.EXPECT().readWithException(gomock.Any(), _ReadFromStore).Return(int64(42), nil).Times(2)
			client.EXPECT().SendHeartbeatToAllBrokerWithLock().Do(func() {
				require.Equal(t, int32(32), pc.option.PullThresholdSizeForQueue.Load())
				require.Equal(t, 2, len(pc.prCh))
			})
			if model == Clustering {
				namesrv := internal.NewMockNamesrvs(ctrl)
				client.SetNameSrv(namesrv)
				namesrv.EXPECT().FindBrokerAddrByTopic("test").Return("broker")
				client.EXPECT().InvokeSync(gomock.Any(), "broker", gomock.Any(), gomock.Any()).Return(&remote.RemotingCommand{Body: []byte(`{"consumerIdList":["test-client"]}`)}, nil)
				client.EXPECT().ClientID().Return("test-client").AnyTimes()
				pc.allocate = func(_ string, _ string, all []*primitive.MessageQueue, _ []string) []*primitive.MessageQueue {
					return all
				}
			}
			pc.doBalance()
		})
	}
}

func TestRebalancePullConsumerDispatchCompatibility(t *testing.T) {
	for _, assigned := range []bool{false, true} {
		name := "subscribe"
		if assigned {
			name = "assign"
		}
		t.Run(name, func(t *testing.T) {
			pc, err := NewPullConsumer(WithInstance(t.Name()), WithConsumerModel(BroadCasting),
				WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})))
			require.NoError(t, err)
			realClient := pc.client
			defer func() { pc.client = realClient; pc.Shutdown() }()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			storage := NewMockOffsetStore(ctrl)
			pc.storage = storage
			queues := flowControlTestQueues(2)
			storage.EXPECT().remove(gomock.Any()).Times(2)
			storage.EXPECT().readWithException(gomock.Any(), _ReadFromStore).Return(int64(42), nil).Times(2)
			if assigned {
				require.NoError(t, pc.Assign("test", queues))
			} else {
				client := internal.NewMockRMQClient(ctrl)
				pc.client = client
				client.EXPECT().SendHeartbeatToAllBrokerWithLock()
				require.NoError(t, pc.Subscribe("test", MessageSelector{}))
				pc.topicSubscribeInfoTable.Store("test", queues)
			}
			pc.Rebalance()
			require.Equal(t, 2, len(pc.prCh))
			require.Len(t, pc.allocateQueues, 2)
			for len(pc.prCh) > 0 {
				require.Equal(t, int64(42), (<-pc.prCh).nextOffset)
			}
		})
	}
}
