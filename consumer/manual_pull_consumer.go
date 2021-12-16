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
	"fmt"
	"strconv"
	"sync"
	"time"

	errors2 "github.com/apache/rocketmq-client-go/v2/errors"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/pkg/errors"
)

// ManualPullConsumer is a low-level consumer, which operates based on MessageQueue.
// Users should maintain information such as offset by themselves
type ManualPullConsumer interface {
	// PullFromQueue return messages according to specified queue with offset
	PullFromQueue(ctx context.Context, mq *primitive.MessageQueue, offset int64, numbers int) (*primitive.PullResult, error)

	// GetMessageQueues return queues of the topic
	GetMessageQueues(ctx context.Context, topic string) ([]*primitive.MessageQueue, error)

	// CommittedOffset return the offset of mq in groupName, if mq not exist, -1 will be return
	CommittedOffset(ctx context.Context, groupName string, mq *primitive.MessageQueue) (int64, error)

	// Seek let consume position to the offset, this api can be used to reset offset and commit offset
	Seek(ctx context.Context, groupName string, mq *primitive.MessageQueue, offset int64) error

	// Lookup return offset according to timestamp(ms), the maximum offset that born time less than timestamp will be return.
	// If timestamp less than any message's born time, the earliest offset will be returned
	// If timestamp great than any message's born time, the latest offset will be returned
	Lookup(ctx context.Context, mq *primitive.MessageQueue, timestamp int64) (int64, error)

	// Shutdown the ManualPullConsumer, clean up internal resources
	Shutdown() error
}

type defaultManualPullConsumer struct {
	namesrv                internal.Namesrvs
	option                 consumerOptions
	client                 internal.RMQClient
	interceptor            primitive.Interceptor
	pullFromWhichNodeTable sync.Map
	shutdownOnce           sync.Once
}

// NewManualPullConsumer creates and initializes a new ManualPullConsumer.
func NewManualPullConsumer(options ...Option) (*defaultManualPullConsumer, error) {
	defaultOpts := defaultPullConsumerOptions()
	for _, apply := range options {
		apply(&defaultOpts)
	}

	srvs, err := internal.NewNamesrv(defaultOpts.Resolver)
	if err != nil {
		return nil, errors.Wrap(err, "new Namesrv failed.")
	}
	if !defaultOpts.Credentials.IsEmpty() {
		srvs.SetCredentials(defaultOpts.Credentials)
	}
	defaultOpts.Namesrv = srvs

	actualRMQClient := internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions, nil)
	actualNameSrv := internal.GetOrSetNamesrv(actualRMQClient.ClientID(), defaultOpts.Namesrv)

	dc := &defaultManualPullConsumer{
		client:  actualRMQClient,
		option:  defaultOpts,
		namesrv: actualNameSrv,
	}

	dc.interceptor = primitive.ChainInterceptors(dc.option.Interceptors...)
	dc.option.ClientOptions.Namesrv = actualNameSrv
	return dc, nil
}

func (dc *defaultManualPullConsumer) PullFromQueue(ctx context.Context, groupName string, mq *primitive.MessageQueue, offset int64, numbers int) (*primitive.PullResult, error) {
	if err := dc.checkPull(ctx, mq, offset, numbers); err != nil {
		return nil, err
	}
	subData := buildSubscriptionData(mq.Topic, MessageSelector{
		Expression: _SubAll,
	})

	sysFlag := buildSysFlag(false, true, true, false)

	pullRequest := &internal.PullMessageRequestHeader{
		ConsumerGroup:        groupName,
		Topic:                mq.Topic,
		QueueId:              int32(mq.QueueId),
		QueueOffset:          offset,
		MaxMsgNums:           int32(numbers),
		SysFlag:              sysFlag,
		CommitOffset:         0,
		SuspendTimeoutMillis: _BrokerSuspendMaxTime,
		SubExpression:        subData.SubString,
		ExpressionType:       string(subData.ExpType),
	}

	if subData.ExpType == string(TAG) {
		pullRequest.SubVersion = 0
	} else {
		pullRequest.SubVersion = subData.SubVersion
	}

	pullResp, err := dc.pullInner(ctx, mq, pullRequest)
	if err != nil {
		return pullResp, err
	}
	dc.processPullResult(mq, pullResp, subData)
	if dc.interceptor != nil {
		msgCtx := &primitive.ConsumeMessageContext{
			Properties:    make(map[string]string),
			ConsumerGroup: groupName,
			MQ:            mq,
			Msgs:          pullResp.GetMessageExts(),
		}
		err = dc.interceptor(ctx, msgCtx, struct{}{}, primitive.NoopInterceptor)
	}
	return pullResp, err
}

func (dc *defaultManualPullConsumer) GetMessageQueues(ctx context.Context, topic string) ([]*primitive.MessageQueue, error) {
	return dc.namesrv.FetchSubscribeMessageQueues(topic)
}

func (dc *defaultManualPullConsumer) CommittedOffset(ctx context.Context, groupName string, mq *primitive.MessageQueue) (int64, error) {
	fn := func(broker string) (*remote.RemotingCommand, error) {
		request := &internal.QueryConsumerOffsetRequestHeader{
			ConsumerGroup: groupName,
			Topic:         mq.Topic,
			QueueId:       mq.QueueId,
		}
		cmd := remote.NewRemotingCommand(internal.ReqGetMaxOffset, request, nil)
		return dc.client.InvokeSync(ctx, broker, cmd, 3*time.Second)
	}
	return dc.processQueryOffset(mq, fn)
}

func (dc *defaultManualPullConsumer) Seek(ctx context.Context, groupName string, mq *primitive.MessageQueue, offset int64) error {
	minOffset, err := dc.queryMinOffset(context.Background(), mq)
	if err != nil {
		return err
	}
	maxOffset, err := dc.queryMaxOffset(context.Background(), mq)
	if err != nil {
		return err
	}
	if offset < minOffset || offset > maxOffset {
		return fmt.Errorf("Seek offset illegal, seek offset = %d, min offset = %d, max offset = %d", offset, minOffset, maxOffset)
	}

	broker, exist := dc.chooseServer(mq)
	if !exist {
		rlog.Warning("the broker does not exist", map[string]interface{}{
			rlog.LogKeyBroker: mq.BrokerName,
		})
		return errors2.ErrBrokerNotFound
	}

	updateOffsetRequest := &internal.UpdateConsumerOffsetRequestHeader{
		ConsumerGroup: groupName,
		Topic:         mq.Topic,
		QueueId:       mq.QueueId,
		CommitOffset:  offset,
	}
	cmd := remote.NewRemotingCommand(internal.ReqUpdateConsumerOffset, updateOffsetRequest, nil)
	return dc.client.InvokeOneWay(context.Background(), broker, cmd, 5*time.Second)
}

func (dc *defaultManualPullConsumer) Lookup(ctx context.Context, mq *primitive.MessageQueue, timestamp int64) (int64, error) {
	fn := func(broker string) (*remote.RemotingCommand, error) {
		request := &internal.SearchOffsetRequestHeader{
			Topic:     mq.Topic,
			QueueId:   mq.QueueId,
			Timestamp: timestamp,
		}
		cmd := remote.NewRemotingCommand(internal.ReqSearchOffsetByTimestamp, request, nil)
		return dc.client.InvokeSync(ctx, broker, cmd, 3*time.Second)
	}
	return dc.processQueryOffset(mq, fn)
}

func (dc *defaultManualPullConsumer) Shutdown() error {
	dc.shutdownOnce.Do(func() {
		dc.client.Shutdown()
	})
	return nil
}

func (dc *defaultManualPullConsumer) chooseServer(mq *primitive.MessageQueue) (string, bool) {
	brokerAddr := dc.namesrv.FindBrokerAddrByName(mq.BrokerName)
	if brokerAddr == "" {
		dc.namesrv.UpdateTopicRouteInfo(mq.Topic)
		brokerAddr = dc.namesrv.FindBrokerAddrByName(mq.BrokerName)
	}
	return brokerAddr, brokerAddr != ""
}

func (dc *defaultManualPullConsumer) queryMinOffset(ctx context.Context, mq *primitive.MessageQueue) (int64, error) {
	fn := func(broker string) (*remote.RemotingCommand, error) {
		request := &internal.GetMinOffsetRequestHeader{
			Topic:   mq.Topic,
			QueueId: mq.QueueId,
		}
		cmd := remote.NewRemotingCommand(internal.ReqGetMinOffset, request, nil)
		return dc.client.InvokeSync(ctx, broker, cmd, 3*time.Second)
	}
	return dc.processQueryOffset(mq, fn)
}

func (dc *defaultManualPullConsumer) queryMaxOffset(ctx context.Context, mq *primitive.MessageQueue) (int64, error) {
	fn := func(broker string) (*remote.RemotingCommand, error) {
		request := &internal.GetMaxOffsetRequestHeader{
			Topic:   mq.Topic,
			QueueId: mq.QueueId,
		}
		cmd := remote.NewRemotingCommand(internal.ReqGetMaxOffset, request, nil)
		return dc.client.InvokeSync(ctx, broker, cmd, 3*time.Second)
	}
	return dc.processQueryOffset(mq, fn)
}

func (dc *defaultManualPullConsumer) processQueryOffset(mq *primitive.MessageQueue, fn func(broker string) (*remote.RemotingCommand, error)) (int64, error) {
	broker, exist := dc.chooseServer(mq)
	if !exist {
		rlog.Warning("the broker does not exist", map[string]interface{}{
			rlog.LogKeyBroker: mq.BrokerName,
		})
		return -1, errors2.ErrBrokerNotFound
	}
	response, err := fn(broker)
	if err != nil {
		return -1, err
	}
	if response.Code != internal.ResSuccess {
		return -2, fmt.Errorf("broker response code: %d, remarks: %s", response.Code, response.Remark)
	}
	off, err := strconv.ParseInt(response.ExtFields["offset"], 10, 64)
	if err != nil {
		return -1, errors.Wrap(err, "parse offset fail.")
	}
	return off, nil
}

func (dc *defaultManualPullConsumer) pullInner(ctx context.Context, mq *primitive.MessageQueue, pullRequest *internal.PullMessageRequestHeader) (*primitive.PullResult, error) {
	brokerResult := dc.tryFindBroker(mq)
	if brokerResult == nil {
		rlog.Warning("no broker found for mq", map[string]interface{}{
			rlog.LogKeyMessageQueue: mq,
		})
		return nil, errors2.ErrBrokerNotFound
	}

	if (pullRequest.ExpressionType == string(TAG)) && brokerResult.BrokerVersion < internal.V4_1_0 {
		return nil, fmt.Errorf("the broker [%s, %v] does not upgrade to support for filter message by %v",
			mq.BrokerName, brokerResult.BrokerVersion, pullRequest.ExpressionType)
	}
	return dc.client.PullMessage(ctx, brokerResult.BrokerAddr, pullRequest)
}

func (dc *defaultManualPullConsumer) tryFindBroker(mq *primitive.MessageQueue) *internal.FindBrokerResult {
	result := dc.namesrv.FindBrokerAddressInSubscribe(mq.BrokerName, dc.recalculatePullFromWhichNode(mq), false)
	if result != nil {
		return result
	}
	dc.namesrv.UpdateTopicRouteInfo(mq.Topic)
	return dc.namesrv.FindBrokerAddressInSubscribe(mq.BrokerName, dc.recalculatePullFromWhichNode(mq), false)
}
func (dc *defaultManualPullConsumer) recalculatePullFromWhichNode(mq *primitive.MessageQueue) int64 {
	v, exist := dc.pullFromWhichNodeTable.Load(*mq)
	if exist {
		return v.(int64)
	}
	return internal.MasterId
}

func (dc *defaultManualPullConsumer) checkPull(ctx context.Context, mq *primitive.MessageQueue, offset int64, numbers int) error {
	if mq == nil {
		return errors2.ErrMQEmpty
	}
	if offset < 0 {
		return errors2.ErrOffset
	}
	if numbers <= 0 {
		return errors2.ErrNumbers
	}
	return nil
}

func (dc *defaultManualPullConsumer) processPullResult(mq *primitive.MessageQueue, result *primitive.PullResult, data *internal.SubscriptionData) {

	dc.pullFromWhichNodeTable.Store(*mq, result.SuggestWhichBrokerId)

	switch result.Status {
	case primitive.PullFound:
		result.SetMessageExts(primitive.DecodeMessage(result.GetBody()))
		msgs := result.GetMessageExts()
		// filter message according to tags
		msgListFilterAgain := msgs
		if data.Tags.Len() > 0 && data.ClassFilterMode {
			msgListFilterAgain = make([]*primitive.MessageExt, 0)
			for _, msg := range msgs {
				_, exist := data.Tags.Contains(msg.GetTags())
				if exist {
					msgListFilterAgain = append(msgListFilterAgain, msg)
				}
			}
		}
		// TODO: add filter message hook
		for _, msg := range msgListFilterAgain {
			traFlag, _ := strconv.ParseBool(msg.GetProperty(primitive.PropertyTransactionPrepared))
			if traFlag {
				msg.TransactionId = msg.GetProperty(primitive.PropertyUniqueClientMessageIdKeyIndex)
			}
			msg.WithProperty(primitive.PropertyMinOffset, strconv.FormatInt(result.MinOffset, 10))
			msg.WithProperty(primitive.PropertyMaxOffset, strconv.FormatInt(result.MaxOffset, 10))
		}
		result.SetMessageExts(msgListFilterAgain)
	}
}
