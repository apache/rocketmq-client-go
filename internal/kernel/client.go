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

package kernel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/internal/remote"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/apache/rocketmq-client-go/rlog"
)

const (
	defaultTraceRegionID = "DefaultRegion"

	// tracing message switch
	_TranceOff = "false"

	// Pulling topic information interval from the named server
	_PullNameServerInterval = 30 * time.Second

	// Pulling topic information interval from the named server
	_HeartbeatBrokerInterval = 30 * time.Second

	// Offset persistent interval for consumer
	_PersistOffset = 5 * time.Second

	// Rebalance interval
	_RebalanceInterval = 10 * time.Second
)

var (
	ErrServiceState = errors.New("service state is not running, please check")

	_VIPChannelEnable = false
)

func init() {
	if os.Getenv("com.rocketmq.sendMessageWithVIPChannel") != "" {
		value, err := strconv.ParseBool(os.Getenv("com.rocketmq.sendMessageWithVIPChannel"))
		if err == nil {
			_VIPChannelEnable = value
		}
	}
}

type InnerProducer interface {
	PublishTopicList() []string
	UpdateTopicPublishInfo(topic string, info *TopicPublishInfo)
	IsPublishTopicNeedUpdate(topic string) bool
	//GetTransactionListener() TransactionListener
	IsUnitMode() bool
}

type InnerConsumer interface {
	PersistConsumerOffset()
	UpdateTopicSubscribeInfo(topic string, mqs []*primitive.MessageQueue)
	IsSubscribeTopicNeedUpdate(topic string) bool
	SubscriptionDataList() []*SubscriptionData
	Rebalance()
	IsUnitMode() bool
}

type RMQClient struct {
	option primitive.ClientOption
	// group -> InnerProducer
	producerMap sync.Map

	// group -> InnerConsumer
	consumerMap sync.Map
	once        sync.Once

	remoteClient *remote.RemotingClient
	hbMutex      sync.Mutex
}

var clientMap sync.Map

func GetOrNewRocketMQClient(option primitive.ClientOption) *RMQClient {
	client := &RMQClient{
		option:       option,
		remoteClient: remote.NewRemotingClient(),
	}
	actual, loaded := clientMap.LoadOrStore(client.ClientID(), client)
	if !loaded {
		client.remoteClient.RegisterRequestFunc(ReqNotifyConsumerIdsChanged, func(req *remote.RemotingCommand) *remote.RemotingCommand {
			rlog.Infof("receive broker's notification, the consumer group: %s", req.ExtFields["consumerGroup"])
			client.RebalanceImmediately()
			return nil
		})
	}
	return actual.(*RMQClient)
}

func (c *RMQClient) Start() {
	c.once.Do(func() {
		// TODO fetchNameServerAddr
		go func() {}()

		// schedule update route info
		go func() {
			// delay
			time.Sleep(50 * time.Millisecond)
			for {
				c.UpdateTopicRouteInfo()
				time.Sleep(_PullNameServerInterval)
			}
		}()

		// TODO cleanOfflineBroker & sendHeartbeatToAllBrokerWithLock
		go func() {
			for {
				cleanOfflineBroker()
				c.SendHeartbeatToAllBrokerWithLock()
				time.Sleep(_HeartbeatBrokerInterval)
			}
		}()

		// schedule persist offset
		go func() {
			//time.Sleep(10 * time.Second)
			for {
				c.consumerMap.Range(func(key, value interface{}) bool {
					consumer := value.(InnerConsumer)
					consumer.PersistConsumerOffset()
					return true
				})
				time.Sleep(_PersistOffset)
			}
		}()

		go func() {
			for {
				c.RebalanceImmediately()
				time.Sleep(_RebalanceInterval)
			}
		}()
	})
}

func (c *RMQClient) Shutdown() {
	// TODO
}

func (c *RMQClient) ClientID() string {
	id := c.option.ClientIP + "@" + c.option.InstanceName
	if c.option.UnitName != "" {
		id += "@" + c.option.UnitName
	}
	return id
}

func (c *RMQClient) InvokeSync(addr string, request *remote.RemotingCommand,
	timeoutMillis time.Duration) (*remote.RemotingCommand, error) {
	return c.remoteClient.InvokeSync(addr, request, timeoutMillis)
}

func (c *RMQClient) InvokeAsync(addr string, request *remote.RemotingCommand,
	timeoutMillis time.Duration, f func(*remote.RemotingCommand, error)) error {
	return c.remoteClient.InvokeAsync(addr, request, timeoutMillis, func(future *remote.ResponseFuture) {
		f(future.ResponseCommand, future.Err)
	})
}

func (c *RMQClient) InvokeOneWay(addr string, request *remote.RemotingCommand,
	timeoutMillis time.Duration) error {
	return c.remoteClient.InvokeOneWay(addr, request, timeoutMillis)
}

func (c *RMQClient) CheckClientInBroker() {
}

// TODO
func (c *RMQClient) SendHeartbeatToAllBrokerWithLock() {
	c.hbMutex.Lock()
	defer c.hbMutex.Unlock()
	hbData := &heartbeatData{
		ClientId: c.ClientID(),
	}
	pData := make([]producerData, 0)
	c.producerMap.Range(func(key, value interface{}) bool {
		pData = append(pData, producerData(key.(string)))
		return true
	})

	cData := make([]consumerData, 0)
	c.consumerMap.Range(func(key, value interface{}) bool {
		consumer := value.(InnerConsumer)
		cData = append(cData, consumerData{
			GroupName:         key.(string),
			CType:             "PUSH",
			MessageModel:      "CLUSTERING",
			Where:             "CONSUME_FROM_FIRST_OFFSET",
			UnitMode:          consumer.IsUnitMode(),
			SubscriptionDatas: consumer.SubscriptionDataList(),
		})
		return true
	})
	hbData.ProducerDatas = pData
	hbData.ConsumerDatas = cData
	if len(pData) == 0 && len(cData) == 0 {
		rlog.Info("sending heartbeat, but no consumer and no consumer")
		return
	}
	brokerAddressesMap.Range(func(key, value interface{}) bool {
		brokerName := key.(string)
		data := value.(*BrokerData)
		for id, addr := range data.BrokerAddresses {
			cmd := remote.NewRemotingCommand(ReqHeartBeat, nil, hbData.encode())
			response, err := c.remoteClient.InvokeSync(addr, cmd, 3*time.Second)
			if err != nil {
				rlog.Warnf("send heart beat to broker error: %s", err.Error())
				return true
			}
			if response.Code == ResSuccess {
				v, exist := brokerVersionMap.Load(brokerName)
				var m map[string]int32
				if exist {
					m = v.(map[string]int32)
				} else {
					m = make(map[string]int32, 4)
					brokerVersionMap.Store(brokerName, m)
				}
				m[brokerName] = int32(response.Version)
				rlog.Infof("send heart beat to broker[%s %d %s] success", brokerName, id, addr)
			}
		}
		return true
	})
}

func (c *RMQClient) UpdateTopicRouteInfo() {
	publishTopicSet := make(map[string]bool, 0)
	c.producerMap.Range(func(key, value interface{}) bool {
		producer := value.(InnerProducer)
		list := producer.PublishTopicList()
		for idx := range list {
			publishTopicSet[list[idx]] = true
		}
		return true
	})
	for topic := range publishTopicSet {
		c.UpdatePublishInfo(topic, UpdateTopicRouteInfo(topic))
	}

	subscribedTopicSet := make(map[string]bool, 0)
	c.consumerMap.Range(func(key, value interface{}) bool {
		consumer := value.(InnerConsumer)
		list := consumer.SubscriptionDataList()
		for idx := range list {
			if !strings.HasPrefix(list[idx].Topic, RetryGroupTopicPrefix) {
				subscribedTopicSet[list[idx].Topic] = true
			}
		}
		return true
	})

	for topic := range subscribedTopicSet {
		c.UpdateSubscribeInfo(topic, UpdateTopicRouteInfo(topic))
	}
}

// SendMessageAsync send message with batch by async
func (c *RMQClient) SendMessageAsync(ctx context.Context, brokerAddrs, brokerName string, request *SendMessageRequest,
	msgs []*primitive.Message, f func(result *primitive.SendResult)) error {
	return nil
}

func (c *RMQClient) SendMessageOneWay(ctx context.Context, brokerAddrs string, request *SendMessageRequest,
	msgs []*primitive.Message) (*primitive.SendResult, error) {
	cmd := remote.NewRemotingCommand(ReqSendBatchMessage, request, encodeMessages(msgs))
	err := c.remoteClient.InvokeOneWay(brokerAddrs, cmd, 3*time.Second)
	if err != nil {
		rlog.Warnf("send messages with oneway error: %v", err)
	}
	return nil, err
}

func (c *RMQClient) ProcessSendResponse(brokerName string, cmd *remote.RemotingCommand, resp *primitive.SendResult, msgs ...*primitive.Message) {
	var status primitive.SendStatus
	switch cmd.Code {
	case ResFlushDiskTimeout:
		status = primitive.SendFlushDiskTimeout
	case ResFlushSlaveTimeout:
		status = primitive.SendFlushSlaveTimeout
	case ResSlaveNotAvailable:
		status = primitive.SendSlaveNotAvailable
	case ResSuccess:
		status = primitive.SendOK
	default:
		// TODO process unknown code
	}

	msgIDs := make([]string, 0)
	for i := 0; i < len(msgs); i++ {
		msgIDs = append(msgIDs, msgs[i].Properties[primitive.PropertyUniqueClientMessageIdKeyIndex])
	}

	regionId := cmd.ExtFields[primitive.PropertyMsgRegion]
	trace := cmd.ExtFields[primitive.PropertyTraceSwitch]

	if regionId == "" {
		regionId = defaultTraceRegionID
	}

	qId, _ := strconv.Atoi(cmd.ExtFields["queueId"])
	off, _ := strconv.ParseInt(cmd.ExtFields["queueOffset"], 10, 64)

	resp.Status = status
	resp.MsgID = cmd.ExtFields["msgId"]
	resp.OffsetMsgID = cmd.ExtFields["msgId"]
	resp.MessageQueue = &primitive.MessageQueue{
		Topic:      msgs[0].Topic,
		BrokerName: brokerName,
		QueueId:    qId,
	}
	resp.QueueOffset = off
	//TransactionID: sendResponse.TransactionId,
	resp.RegionID = regionId
	resp.TraceOn = trace != "" && trace != _TranceOff

}

// PullMessage with sync
func (c *RMQClient) PullMessage(ctx context.Context, brokerAddrs string, request *PullMessageRequest) (*primitive.PullResult, error) {
	cmd := remote.NewRemotingCommand(ReqPullMessage, request, nil)
	res, err := c.remoteClient.InvokeSync(brokerAddrs, cmd, 3*time.Second)
	if err != nil {
		return nil, err
	}

	return c.processPullResponse(res)
}

func (c *RMQClient) processPullResponse(response *remote.RemotingCommand) (*primitive.PullResult, error) {

	pullResult := &primitive.PullResult{}
	switch response.Code {
	case ResSuccess:
		pullResult.Status = primitive.PullFound
	case ResPullNotFound:
		pullResult.Status = primitive.PullNoNewMsg
	case ResPullRetryImmediately:
		pullResult.Status = primitive.PullNoMsgMatched
	case ResPullOffsetMoved:
		pullResult.Status = primitive.PullOffsetIllegal
	default:
		return nil, fmt.Errorf("unknown Response Code: %d, remark: %s", response.Code, response.Remark)
	}

	c.decodeCommandCustomHeader(pullResult, response)
	pullResult.SetBody(response.Body)

	return pullResult, nil
}

func (c *RMQClient) decodeCommandCustomHeader(pr *primitive.PullResult, cmd *remote.RemotingCommand) {
	v, exist := cmd.ExtFields["maxOffset"]
	if exist {
		pr.MaxOffset, _ = strconv.ParseInt(v, 10, 64)
	}

	v, exist = cmd.ExtFields["minOffset"]
	if exist {
		pr.MinOffset, _ = strconv.ParseInt(v, 10, 64)
	}

	v, exist = cmd.ExtFields["nextBeginOffset"]
	if exist {
		pr.NextBeginOffset, _ = strconv.ParseInt(v, 10, 64)
	}

	v, exist = cmd.ExtFields["suggestWhichBrokerId"]
	if exist {
		pr.SuggestWhichBrokerId, _ = strconv.ParseInt(v, 10, 64)
	}
}

// PullMessageAsync pull message async
func (c *RMQClient) PullMessageAsync(ctx context.Context, brokerAddrs string, request *PullMessageRequest, f func(result *primitive.PullResult)) error {
	return nil
}

func (c *RMQClient) RegisterConsumer(group string, consumer InnerConsumer) error {
	c.consumerMap.Store(group, consumer)
	return nil
}

func (c *RMQClient) UnregisterConsumer(group string) {
}

func (c *RMQClient) RegisterProducer(group string, producer InnerProducer) {
	c.producerMap.Store(group, producer)
}

func (c *RMQClient) UnregisterProducer(group string) {
}

func (c *RMQClient) SelectProducer(group string) InnerProducer {
	return nil
}

func (c *RMQClient) SelectConsumer(group string) InnerConsumer {
	return nil
}

func (c *RMQClient) RebalanceImmediately() {
	c.consumerMap.Range(func(key, value interface{}) bool {
		consumer := value.(InnerConsumer)
		consumer.Rebalance()
		return true
	})
}

func (c *RMQClient) UpdatePublishInfo(topic string, data *TopicRouteData) {
	if data == nil {
		return
	}
	if !c.isNeedUpdatePublishInfo(topic) {
		return
	}
	c.producerMap.Range(func(key, value interface{}) bool {
		p := value.(InnerProducer)
		publishInfo := routeData2PublishInfo(topic, data)
		publishInfo.HaveTopicRouterInfo = true
		p.UpdateTopicPublishInfo(topic, publishInfo)
		return true
	})
}

func (c *RMQClient) isNeedUpdatePublishInfo(topic string) bool {
	var result bool
	c.producerMap.Range(func(key, value interface{}) bool {
		p := value.(InnerProducer)
		if p.IsPublishTopicNeedUpdate(topic) {
			result = true
			return false
		}
		return true
	})
	return result
}

func (c *RMQClient) UpdateSubscribeInfo(topic string, data *TopicRouteData) {
	if data == nil {
		return
	}
	if !c.isNeedUpdateSubscribeInfo(topic) {
		return
	}
	c.consumerMap.Range(func(key, value interface{}) bool {
		consumer := value.(InnerConsumer)
		// TODO
		consumer.UpdateTopicSubscribeInfo(topic, routeData2SubscribeInfo(topic, data))
		return true
	})
}

func (c *RMQClient) isNeedUpdateSubscribeInfo(topic string) bool {
	var result bool
	c.consumerMap.Range(func(key, value interface{}) bool {
		consumer := value.(InnerConsumer)
		if consumer.IsSubscribeTopicNeedUpdate(topic) {
			result = true
			return false
		}
		return true
	})
	return result
}

func routeData2SubscribeInfo(topic string, data *TopicRouteData) []*primitive.MessageQueue {
	list := make([]*primitive.MessageQueue, 0)
	for idx := range data.QueueDataList {
		qd := data.QueueDataList[idx]
		if queueIsReadable(qd.Perm) {
			for i := 0; i < qd.ReadQueueNums; i++ {
				list = append(list, &primitive.MessageQueue{
					Topic:      topic,
					BrokerName: qd.BrokerName,
					QueueId:    i,
				})
			}
		}
	}
	return list
}

func encodeMessages(message []*primitive.Message) []byte {
	var buffer bytes.Buffer
	index := 0
	for index < len(message) {
		buffer.Write(message[index].Body)
	}
	return buffer.Bytes()
}

func brokerVIPChannel(brokerAddr string) string {
	if !_VIPChannelEnable {
		return brokerAddr
	}
	var brokerAddrNew strings.Builder
	ipAndPort := strings.Split(brokerAddr, ":")
	port, err := strconv.Atoi(ipAndPort[1])
	if err != nil {
		return ""
	}
	brokerAddrNew.WriteString(ipAndPort[0])
	brokerAddrNew.WriteString(":")
	brokerAddrNew.WriteString(strconv.Itoa(port - 2))
	return brokerAddrNew.String()
}
