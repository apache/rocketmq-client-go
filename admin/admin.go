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

package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

var (
	ErrNotFound = errors.New("no msg found")
)

type QueueOffset struct {
	*primitive.MessageQueue
	Offset int64
}

func (offset *QueueOffset) String() string {
	if offset == nil {
		return fmt.Sprintf("queueOffset is nil")
	} else {
		return fmt.Sprintf("MessageQueue [Topic=%s, brokerName=%s, queueId=%d] offset: %d", offset.Topic, offset.BrokerName, offset.QueueId, offset.Offset)
	}
}

type QueueErr struct {
	*primitive.MessageQueue
	Err error
}

func (queueErr *QueueErr) String() string {
	if queueErr == nil {
		return fmt.Sprintf("queueOffset is nil")
	} else {
		return fmt.Sprintf("MessageQueue [Topic=%s, brokerName=%s, queueId=%d] offset: %d", queueErr.Topic, queueErr.BrokerName, queueErr.QueueId, queueErr.Err)
	}
}

type Admin interface {
	FetchConsumerOffsets(ctx context.Context, topic string, group string) ([]QueueOffset, error)
	FetchConsumerOffset(ctx context.Context, group string, mq *primitive.MessageQueue) (int64, error)

	SearchOffsets(ctx context.Context, topic string, expected time.Time) ([]QueueOffset, error)
	SearchOffset(ctx context.Context, expected time.Time, mq *primitive.MessageQueue) (int64, error)

	ResetConsumerOffsets(ctx context.Context, topic string, group string, offset int64) ([]QueueErr, error)
	ResetConsumerOffset(ctx context.Context, group string, mq *primitive.MessageQueue, offset int64) error

	MinOffsets(ctx context.Context, topic string) ([]QueueOffset, error)
	MinOffset(ctx context.Context, mq *primitive.MessageQueue) (int64, error)

	MaxOffsets(ctx context.Context, topic string) ([]QueueOffset, error)
	MaxOffset(ctx context.Context, mq *primitive.MessageQueue) (int64, error)

	SearchKey(ctx context.Context, topic string, key string, maxNum int) ([]*primitive.MessageExt, error)

	ViewMessageByPhyOffsets(ctx context.Context, topic string, offset int64) ([]*primitive.MessageExt, error)
	ViewMessageByPhyOffset(ctx context.Context, mq *primitive.MessageQueue, offset int64) ([]*primitive.MessageExt, error)

	ViewMessageByQueueOffset(ctx context.Context, queue *primitive.MessageQueue, offset int64) (*primitive.MessageExt, error)

	GetConsumerConnectionList(ctx context.Context, group string) (*ConsumerConnection, error)
	GetConsumerIdList(ctx context.Context, group string) ([]string, error)
	GetConsumerRunningInfo(ctx context.Context, group string, clientID string) (*internal.ConsumerRunningInfo, error)
	Allocation(ctx context.Context, group string) (map[primitive.MessageQueue]string, error)

	Close() error
}

// TODO: 超时的内容, 全部转移到 ctx
type adminOptions struct {
	internal.ClientOptions
}

type AdminOption func(options *adminOptions)

func defaultAdminOptions() *adminOptions {
	opts := &adminOptions{
		ClientOptions: internal.DefaultClientOptions(),
	}
	opts.GroupName = "TOOLS_CONSUMER"
	opts.InstanceName = time.Now().String()
	return opts
}

// WithResolver nameserver resolver to fetch nameserver addr
func WithResolver(resolver primitive.NsResolver) AdminOption {
	return func(options *adminOptions) {
		options.Resolver = resolver
	}
}

// WithGroupName consumer group name
func WithGroupName(groupName string) AdminOption {
	return func(options *adminOptions) {
		options.GroupName = groupName
	}
}

type admin struct {
	cli     internal.RMQClient
	namesrv internal.Namesrvs

	opts *adminOptions

	closeOnce sync.Once
}

// NewAdmin initialize admin
func NewAdmin(opts ...AdminOption) (Admin, error) {
	defaultOpts := defaultAdminOptions()
	for _, opt := range opts {
		opt(defaultOpts)
	}

	cli := internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions, nil)
	namesrv, err := internal.NewNamesrv(defaultOpts.Resolver)
	if err != nil {
		return nil, err
	}

	return &admin{
		cli:     cli,
		namesrv: namesrv,
		opts:    defaultOpts,
	}, nil
}

func (a *admin) parallel(ctx context.Context, topic string, task func(mq *primitive.MessageQueue)) error {
	mqs, err := a.namesrv.FetchPublishMessageQueues(topic)
	if err != nil {
		return err
	}
	var group errgroup.Group
	for _, mq := range mqs {
		tmp := mq
		group.Go(func() error {
			task(tmp)
			return nil
		})
	}
	return group.Wait()
}

// FetchConsumerOffsets parallel version of FetchConsumerOffset for multi mq, return Offset=-1 if single `FetchConsumerOffset` failed.
func (a *admin) FetchConsumerOffsets(ctx context.Context, topic string, group string) ([]QueueOffset, error) {
	var queueOffsets []QueueOffset
	var lock sync.Mutex

	err := a.parallel(ctx, topic, func(mq *primitive.MessageQueue) {
		offset, err := a.FetchConsumerOffset(ctx, group, mq)
		if err != nil {
			offset = -1
		}

		lock.Lock()
		queueOffset := QueueOffset{
			MessageQueue: mq,
			Offset:       offset,
		}
		queueOffsets = append(queueOffsets, queueOffset)
		lock.Unlock()
	})
	if err != nil {
		return nil, err
	}

	return queueOffsets, nil
}

// FetchConsumerOffset fetch consumer offset of speified queue,  use FetchConsumerOffsets to get topic offset of group
func (a *admin) FetchConsumerOffset(ctx context.Context, group string, mq *primitive.MessageQueue) (int64, error) {
	broker, err := a.getAddr(mq)
	if err != nil {
		return -1, err
	}

	queryOffsetRequest := &internal.QueryConsumerOffsetRequestHeader{
		ConsumerGroup: group,
		Topic:         mq.Topic,
		QueueId:       mq.QueueId,
	}
	cmd := remote.NewRemotingCommand(internal.ReqQueryConsumerOffset, queryOffsetRequest, nil)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	res, err := a.cli.InvokeSync(ctx, broker, cmd, 3*time.Second)
	if err != nil {
		return -1, err
	}

	if res.Code != internal.ResSuccess {
		return -2, fmt.Errorf("broker response code: %d, remarks: %s", res.Code, res.Remark)
	}
	return strconv.ParseInt(res.ExtFields["offset"], 10, 64)
}

// SearchOffsets parallel version of SearchOffset
func (a *admin) SearchOffsets(ctx context.Context, topic string, expected time.Time) ([]QueueOffset, error) {
	var queueOffsets []QueueOffset
	var lock sync.Mutex

	err := a.parallel(ctx, topic, func(mq *primitive.MessageQueue) {
		offset, err := a.SearchOffset(ctx, expected, mq)
		if err != nil {
			offset = -1
		}

		lock.Lock()
		queueOffset := QueueOffset{
			MessageQueue: mq,
			Offset:       offset,
		}
		queueOffsets = append(queueOffsets, queueOffset)
		lock.Unlock()
	})
	if err != nil {
		return nil, err
	}

	return queueOffsets, nil
}

// SearchOffset get queue offset of speficield time
func (a *admin) SearchOffset(ctx context.Context, expected time.Time, mq *primitive.MessageQueue) (int64, error) {
	addr, err := a.getAddr(mq)
	if err != nil {
		return -1, err
	}

	request := &internal.SearchOffsetRequestHeader{
		Topic:     mq.Topic,
		QueueId:   mq.QueueId,
		Timestamp: expected.Unix() * 1000,
	}

	cmd := remote.NewRemotingCommand(internal.ReqSearchOffsetByTimestamp, request, nil)
	response, err := a.cli.InvokeSync(ctx, addr, cmd, 10*time.Second)
	if err != nil {
		rlog.Error("invoke sync failed", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return -1, err
	}

	return strconv.ParseInt(response.ExtFields["offset"], 10, 64)
}

// ResetConsumerOffsets result offset of group of specified topic
func (a *admin) ResetConsumerOffsets(ctx context.Context, topic string, group string, offset int64) ([]QueueErr, error) {
	var queueErrs []QueueErr
	var lock sync.Mutex

	err := a.parallel(ctx, topic, func(mq *primitive.MessageQueue) {
		err := a.ResetConsumerOffset(ctx, group, mq, offset)

		lock.Lock()
		queueErr := QueueErr{
			MessageQueue: mq,
			Err:          err,
		}
		queueErrs = append(queueErrs, queueErr)
		lock.Unlock()
	})
	if err != nil {
		return nil, err
	}

	return queueErrs, nil
}

// ResetConsumerOffset reset offset back or forward to specified offset
func (a *admin) ResetConsumerOffset(ctx context.Context, group string, mq *primitive.MessageQueue, offset int64) error {
	broker, err := a.getAddr(mq)
	if err != nil {
		return err
	}

	updateOffsetRequest := &internal.UpdateConsumerOffsetRequestHeader{
		ConsumerGroup: group,
		Topic:         mq.Topic,
		QueueId:       mq.QueueId,
		CommitOffset:  offset,
	}
	cmd := remote.NewRemotingCommand(internal.ReqUpdateConsumerOffset, updateOffsetRequest, nil)
	return a.cli.InvokeOneWay(ctx, broker, cmd, 5*time.Second)
}

// SearchKey search key from topic, return empty slice if no msg found
func (a *admin) SearchKey(ctx context.Context, topic string, key string, maxNum int) ([]*primitive.MessageExt, error) {
	topicRoute, _, err := a.namesrv.UpdateTopicRouteInfo(topic)
	if err != nil {
		return nil, err
	}
	addrs := make(map[string]string)
	for _, broker := range topicRoute.BrokerDataList {
		slaves := broker.GetSlaves()
		if len(slaves) > 0 {
			addr := slaves[0]
			addrs[broker.BrokerName] = addr
			continue
		}

		master := broker.MasterAddr()
		if len(master) > 0 {
			addrs[broker.BrokerName] = master
			continue
		}
	}

	if len(addrs) == 0 {
		return nil, fmt.Errorf("no broker found for topic: %s", topic)
	}

	queryMsgs := make([]*primitive.MessageExt, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(addrs))
	for b, baddr := range addrs {
		brokerName, addr := b, baddr
		header := &internal.QueryMessageRequestHeader{
			Topic:          topic,
			Key:            key,
			MaxNum:         maxNum,
			BeginTimestamp: 0,
			EndTimestamp:   math.MaxInt64,
		}
		cmd := remote.NewRemotingCommand(internal.ReqQueryMessage, header, nil)
		cmd.ExtFields["_UNIQUE_KEY_QUERY"] = "false"
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := a.cli.InvokeAsync(ctx, addr, cmd, func(command *remote.RemotingCommand, e error) {
			cancel()
			if e != nil {
				rlog.Error(fmt.Sprintf("invoke %s failed", addr), map[string]interface{}{
					rlog.LogKeyUnderlayError: err,
				})
				return
			}
			switch command.Code {
			case internal.ResSuccess:
				lock.Lock()
				msgs := primitive.DecodeMessage(command.Body)
				for _, msg := range msgs {
					msg.Queue = &primitive.MessageQueue{
						Topic:      topic,
						BrokerName: brokerName,
						QueueId:    int(msg.QueueId),
					}
					msg.StoreHost = addr
				}

				for _, msg := range msgs {
					keys := msg.GetKeys()
					kiter := strings.Split(keys, primitive.PropertyKeySeparator)
					for _, k := range kiter {
						if k == key {
							queryMsgs = append(queryMsgs, msg)
						}
					}
				}
				lock.Unlock()
			default:
				rlog.Warning(fmt.Sprintf("invoke addr: %s failed with code: %d", addr, command.Code),
					map[string]interface{}{
						rlog.LogKeyUnderlayError: err,
					})
			}
			wg.Done()
		})
		if err != nil {
			cancel()
		}
	}
	wg.Wait()
	return queryMsgs, nil
}

// MinOffsets parallel version of MinOffset for multi mq
func (a *admin) MinOffsets(ctx context.Context, topic string) ([]QueueOffset, error) {
	var queueOffsets []QueueOffset
	var lock sync.Mutex

	err := a.parallel(ctx, topic, func(mq *primitive.MessageQueue) {
		offset, err := a.MinOffset(ctx, mq)
		if err != nil {
			offset = -1
		}

		lock.Lock()
		queueOffset := QueueOffset{
			MessageQueue: mq,
			Offset:       offset,
		}
		queueOffsets = append(queueOffsets, queueOffset)
		lock.Unlock()
	})
	if err != nil {
		return nil, err
	}

	return queueOffsets, nil
}

// MinOffset get min offset of specified queue, return Offset=-1 if single `MinOffset` failed.
func (a *admin) MinOffset(ctx context.Context, mq *primitive.MessageQueue) (int64, error) {
	brokerAddr, err := a.getAddr(mq)
	if err != nil {
		return -1, err
	}

	request := &internal.GetMinOffsetRequestHeader{
		Topic:   mq.Topic,
		QueueId: mq.QueueId,
	}

	cmd := remote.NewRemotingCommand(internal.ReqGetMinOffset, request, nil)
	// TODO: zero.xu remove extra timeout param
	response, err := a.cli.InvokeSync(ctx, brokerAddr, cmd, 3*time.Second)
	if err != nil {
		return -1, err
	}

	return strconv.ParseInt(response.ExtFields["offset"], 10, 64)
}

// MaxOffsets parallel version of MaxOffsets for multi mq
func (a *admin) MaxOffsets(ctx context.Context, topic string) ([]QueueOffset, error) {
	var queueOffsets []QueueOffset
	var lock sync.Mutex

	err := a.parallel(ctx, topic, func(mq *primitive.MessageQueue) {
		offset, err := a.MaxOffset(ctx, mq)
		if err != nil {
			rlog.Error("maxOffset get failed", map[string]interface{}{
				rlog.LogKeyUnderlayError: err,
			})
			// mq failed.
			offset = -1
		}

		lock.Lock()
		queueOffset := QueueOffset{
			MessageQueue: mq,
			Offset:       offset,
		}
		queueOffsets = append(queueOffsets, queueOffset)
		lock.Unlock()
	})
	if err != nil {
		return nil, err
	}

	return queueOffsets, nil
}

// MaxOffset fetch max offset of specified queue, return Offset=-1 if single `MaxOffsets` failed
func (a *admin) MaxOffset(ctx context.Context, mq *primitive.MessageQueue) (int64, error) {
	brokerAddr, err := a.getAddr(mq)
	if err != nil {
		return -1, err
	}

	request := &internal.GetMaxOffsetRequestHeader{
		Topic:   mq.Topic,
		QueueId: mq.QueueId,
	}

	cmd := remote.NewRemotingCommand(internal.ReqGetMaxOffset, request, nil)
	response, err := a.cli.InvokeSync(ctx, brokerAddr, cmd, 3*time.Second)
	if err != nil {
		return -1, err
	}

	return strconv.ParseInt(response.ExtFields["offset"], 10, 64)
}

func (a *admin) getAddr(mq *primitive.MessageQueue) (string, error) {
	broker := a.namesrv.FindBrokerAddrByName(mq.BrokerName)
	if len(broker) == 0 {
		a.namesrv.UpdateTopicRouteInfo(mq.Topic)
		broker = a.namesrv.FindBrokerAddrByName(mq.BrokerName)

		if len(broker) == 0 {
			return "", fmt.Errorf("broker: %s address not found", mq.BrokerName)
		}
	}
	return broker, nil
}

// ViewMessageByPhyOffsets parallel version of ViewMessageByPhyOffset
func (a *admin) ViewMessageByPhyOffsets(ctx context.Context, topic string, offset int64) ([]*primitive.MessageExt, error) {
	var msgs []*primitive.MessageExt
	var lock sync.Mutex

	err := a.parallel(ctx, topic, func(mq *primitive.MessageQueue) {
		queueMsgs, err := a.ViewMessageByPhyOffset(ctx, mq, offset)
		if err != nil {
			rlog.Error(fmt.Sprintf("maxOffset get failed"), map[string]interface{}{
				rlog.LogKeyUnderlayError: err,
			})
		}

		lock.Lock()
		msgs = append(msgs, queueMsgs...)
		lock.Unlock()
	})
	if err != nil {
		return nil, err
	}

	return msgs, nil
}

// ViewMessageByPhyOffset get message by commitlog offset
func (a *admin) ViewMessageByPhyOffset(ctx context.Context, mq *primitive.MessageQueue, offset int64) ([]*primitive.MessageExt, error) {
	brokerAddr, err := a.getAddr(mq)
	if err != nil {
		return nil, err
	}

	request := &internal.ViewMessageRequestHeader{
		Offset: offset,
	}

	cmd := remote.NewRemotingCommand(internal.ReqViewMessageByID, request, nil)
	// TODO: zero.xu remove timeout later
	response, err := a.cli.InvokeSync(ctx, brokerAddr, cmd, 3*time.Second)
	if err != nil {
		return nil, err
	}

	switch response.Code {
	case internal.ResSuccess:
		msgs := primitive.DecodeMessage(response.Body)
		return msgs, nil
	default:
		rlog.Warning(fmt.Sprintf("unexpected response code: %d", response.Code), nil)
	}

	return nil, nil
}

// ViewMessageByQueueOffset get message of specified offset of queue
func (a *admin) ViewMessageByQueueOffset(ctx context.Context, queue *primitive.MessageQueue, offset int64) (*primitive.MessageExt, error) {
	brokerAddr, err := a.getAddr(queue)
	if err != nil {
		return nil, err
	}
	sysFlag := consumer.BuildSysFlag(false, true, true, false)
	data := consumer.BuildSubscriptionData(queue.Topic, consumer.MessageSelector{})

	pullRequest := &internal.PullMessageRequestHeader{
		ConsumerGroup:        a.opts.GroupName,
		Topic:                queue.Topic,
		QueueId:              int32(queue.QueueId),
		QueueOffset:          offset,
		MaxMsgNums:           int32(1),
		SysFlag:              sysFlag,
		CommitOffset:         0,
		SuspendTimeoutMillis: 0,
		SubExpression:        data.SubString,
		SubVersion:           0,
		ExpressionType:       data.ExpType,
	}

	res, err := a.cli.PullMessage(ctx, brokerAddr, pullRequest)
	if err != nil {
		return nil, err
	}
	switch res.Status {
	case primitive.PullFound:
		msgs := primitive.DecodeMessage(res.GetBody())
		if len(msgs) == 0 {
			return nil, ErrNotFound
		}
		msgs[0].StoreHost = brokerAddr
		msgs[0].Queue = queue
		return msgs[0], nil
	default:
	}
	return nil, ErrNotFound
}

// Allocation get partition-client allocation info of group
func (a *admin) Allocation(ctx context.Context, group string) (map[primitive.MessageQueue]string, error) {

	ids, err := a.GetConsumerIdList(ctx, group)
	if err != nil {
		return nil, err
	}
	alloc := make(map[primitive.MessageQueue]string)
	retryTopic := "%RETRY%" + group
	for i := range ids {
		id := ids[i]
		runningInfo, err := a.GetConsumerRunningInfo(ctx, group, id)
		if err != nil {
			return alloc, err
		}
		for k := range runningInfo.MQTable {
			if k.Topic == retryTopic {
				continue
			}
			alloc[k] = id
		}
	}
	return alloc, nil
}

// GetConsumerConnectionList get all client connection info of group
func (a *admin) GetConsumerConnectionList(ctx context.Context, group string) (*ConsumerConnection, error) {
	retryTopic := internal.GetRetryTopic(group)
	topicRoute, _, err := a.namesrv.UpdateTopicRouteInfo(retryTopic)
	if err != nil {
		return nil, err
	}
	i := rand.Intn(len(topicRoute.BrokerDataList))
	brokerAddr := topicRoute.BrokerDataList[i].SelectBrokerAddr()

	req := &internal.GetConsumerListRequestHeader{
		ConsumerGroup: group,
	}
	cmd := remote.NewRemotingCommand(internal.ReqGetConsumerListByGroup, req, nil)
	response, err := a.cli.InvokeSync(ctx, brokerAddr, cmd, 10*time.Second)
	if err != nil {
		return nil, err
	}

	switch response.Code {
	case internal.ResSuccess:
		c := &ConsumerConnection{}
		err = json.Unmarshal(response.Body, c)
		if err != nil {
			rlog.Error("unmarshal consumer list info failed", map[string]interface{}{
				rlog.LogKeyUnderlayError: err,
			})
		}
		return c, nil
	default:
		rlog.Warning(fmt.Sprintf("unexpected response code: %d", response.Code), nil)
	}
	return nil, primitive.MQBrokerErr{ResponseCode: response.Code, ErrorMessage: response.Remark}
}

// GetConsumerIdList get all group client id
func (a *admin) GetConsumerIdList(ctx context.Context, group string) ([]string, error) {
	retryTopic := internal.GetRetryTopic(group)
	topicRoute, _, err := a.namesrv.UpdateTopicRouteInfo(retryTopic)
	if err != nil {
		return nil, err
	}
	i := rand.Intn(len(topicRoute.BrokerDataList))
	brokerAddr := topicRoute.BrokerDataList[i].SelectBrokerAddr()

	req := &internal.GetConsumerListRequestHeader{
		ConsumerGroup: group,
	}
	cmd := remote.NewRemotingCommand(internal.ReqGetConsumerListByGroup, req, nil)
	// TODO: xujianhai666 remove timeout later
	response, err := a.cli.InvokeSync(ctx, brokerAddr, cmd, 10*time.Second)
	if err != nil {
		return nil, err
	}

	switch response.Code {
	case internal.ResSuccess:
		result := gjson.ParseBytes(response.Body)
		list := make([]string, 0)
		arr := result.Get("consumerIdList").Array()
		for idx := range arr {
			list = append(list, arr[idx].String())
		}
		return list, nil
	default:
		rlog.Error(fmt.Sprintf("unexpected response code: %d", response.Code), nil)
	}
	return nil, primitive.MQBrokerErr{ResponseCode: response.Code, ErrorMessage: response.Remark}
}

// GetConsumerRunningInfo fetch running info of speified client of group
func (a *admin) GetConsumerRunningInfo(ctx context.Context, group string, clientID string) (*internal.ConsumerRunningInfo, error) {
	retryTopic := internal.GetRetryTopic(group)
	topicRoute, _, err := a.namesrv.UpdateTopicRouteInfo(retryTopic)
	if err != nil {
		return nil, err
	}
	i := rand.Intn(len(topicRoute.BrokerDataList))
	brokerAddr := topicRoute.BrokerDataList[i].SelectBrokerAddr()

	req := &internal.GetConsumerRunningInfoRequestHeader{
		ConsumerGroup: group,
		ClientID:      clientID,
	}
	cmd := remote.NewRemotingCommand(internal.ReqGetConsumerRunningInfo, req, nil)
	response, err := a.cli.InvokeSync(ctx, brokerAddr, cmd, 10*time.Second)
	if err != nil {
		return nil, err
	}

	switch response.Code {
	case internal.ResSuccess:
		info := &internal.ConsumerRunningInfo{}
		err := info.Decode(response.Body)
		if err != nil {
			rlog.Error("unmarshal failed", map[string]interface{}{
				rlog.LogKeyUnderlayError: err,
			})
			return nil, err
		}
		return info, nil
	default:
		rlog.Error("unmarshal failed", nil)
	}
	return nil, primitive.MQBrokerErr{ResponseCode: response.Code, ErrorMessage: response.Remark}
}

func (a *admin) Close() error {
	a.closeOnce.Do(func() {
		a.cli.Shutdown()
	})
	return nil
}
