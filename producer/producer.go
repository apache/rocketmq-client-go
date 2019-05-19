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
	"context"
	"errors"
	"fmt"
	"github.com/apache/rocketmq-client-go/kernel"
	"github.com/apache/rocketmq-client-go/remote"
	"github.com/apache/rocketmq-client-go/rlog"
	"github.com/apache/rocketmq-client-go/utils"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Producer interface {
	Start() error
	Shutdown() error
	SendSync(context.Context, *kernel.Message) (*kernel.SendResult, error)
	SendOneWay(context.Context, *kernel.Message) error
}

func NewProducer(opt ProducerOptions) (Producer, error) {
	if err := utils.VerifyIP(opt.NameServerAddr); err != nil {
		return nil, err
	}
	if opt.RetryTimesWhenSendFailed == 0 {
		opt.RetryTimesWhenSendFailed = 2
	}
	if opt.NameServerAddr == "" {
		rlog.Fatal("opt.NameServerAddr can't be empty")
	}
	err := os.Setenv(kernel.EnvNameServerAddr, opt.NameServerAddr)
	if err != nil {
		rlog.Fatal("set env=EnvNameServerAddr error: %s ", err.Error())
	}
	return &defaultProducer{
		group:   "default",
		client:  kernel.GetOrNewRocketMQClient(opt.ClientOption),
		options: opt,
	}, nil
}

type defaultProducer struct {
	group       string
	client      *kernel.RMQClient
	state       kernel.ServiceState
	options     ProducerOptions
	publishInfo sync.Map
}

type ProducerOptions struct {
	kernel.ClientOption
	NameServerAddr           string
	GroupName                string
	RetryTimesWhenSendFailed int
	UnitMode                 bool
}

func (p *defaultProducer) Start() error {
	p.state = kernel.StateRunning
	p.client.RegisterProducer(p.group, p)
	p.client.Start()
	return nil
}

func (p *defaultProducer) Shutdown() error {
	return nil
}

func (p *defaultProducer) SendSync(ctx context.Context, msg *kernel.Message) (*kernel.SendResult, error) {
	if msg == nil {
		return nil, errors.New("message is nil")
	}

	if msg.Topic == "" {
		return nil, errors.New("topic is nil")
	}

	retryTime := 1 + p.options.RetryTimesWhenSendFailed

	var (
		err error
	)
	for retryCount := 0; retryCount < retryTime; retryCount++ {
		mq := p.selectMessageQueue(msg.Topic)
		if mq == nil {
			err = fmt.Errorf("the topic=%s route info not found", msg.Topic)
			continue
		}

		addr := kernel.FindBrokerAddrByName(mq.BrokerName)
		if addr == "" {
			return nil, fmt.Errorf("topic=%s route info not found", mq.Topic)
		}

		res, _err := p.client.InvokeSync(addr, p.buildSendRequest(mq, msg), 3*time.Second)
		if _err != nil {
			err = _err
			continue
		}
		return p.client.ProcessSendResponse(mq.BrokerName, res, msg), nil
	}
	return nil, err
}

func (p *defaultProducer) SendOneWay(ctx context.Context, msg *kernel.Message) error {
	if msg == nil {
		return errors.New("message is nil")
	}

	if msg.Topic == "" {
		return errors.New("topic is nil")
	}

	retryTime := 1 + p.options.RetryTimesWhenSendFailed

	var (
		err error
	)
	for retryCount := 0; retryCount < retryTime; retryCount++ {
		mq := p.selectMessageQueue(msg.Topic)
		if mq == nil {
			err = fmt.Errorf("the topic=%s route info not found", msg.Topic)
			continue
		}

		addr := kernel.FindBrokerAddrByName(mq.BrokerName)
		if addr == "" {
			return fmt.Errorf("topic=%s route info not found", mq.Topic)
		}

		_err := p.client.InvokeOneWay(addr, p.buildSendRequest(mq, msg), 3*time.Second)
		if _err != nil {
			err = _err
			continue
		}
	}
	return err
}

func (p *defaultProducer) buildSendRequest(mq *kernel.MessageQueue, msg *kernel.Message) *remote.RemotingCommand {
	req := &kernel.SendMessageRequest{
		ProducerGroup:  p.group,
		Topic:          mq.Topic,
		QueueId:        mq.QueueId,
		SysFlag:        0,
		BornTimestamp:  time.Now().UnixNano() / int64(time.Millisecond),
		Flag:           msg.Flag,
		Properties:     propertiesToString(msg.Properties),
		ReconsumeTimes: 0,
		UnitMode:       p.options.UnitMode,
		Batch:          false,
	}
	return remote.NewRemotingCommand(kernel.ReqSendMessage, req, msg.Body)
}

func (p *defaultProducer) selectMessageQueue(topic string) *kernel.MessageQueue {
	v, exist := p.publishInfo.Load(topic)

	if !exist {
		p.client.UpdatePublishInfo(topic, kernel.UpdateTopicRouteInfo(topic))
		v, exist = p.publishInfo.Load(topic)
	}

	if !exist {
		return nil
	}

	result := v.(*kernel.TopicPublishInfo)
	if result == nil || !result.HaveTopicRouterInfo {
		return nil
	}

	if result.MqList != nil && len(result.MqList) <= 0 {
		rlog.Error("can not find proper message queue")
		return nil
	}
	return result.MqList[int(atomic.AddInt32(&result.TopicQueueIndex, 1))%len(result.MqList)]
}

func (p *defaultProducer) PublishTopicList() []string {
	topics := make([]string, 0)
	p.publishInfo.Range(func(key, value interface{}) bool {
		topics = append(topics, key.(string))
		return true
	})
	return topics
}

func (p *defaultProducer) UpdateTopicPublishInfo(topic string, info *kernel.TopicPublishInfo) {
	if topic == "" || info == nil {
		return
	}
	p.publishInfo.Store(topic, info)
}

func (p *defaultProducer) IsPublishTopicNeedUpdate(topic string) bool {
	v, exist := p.publishInfo.Load(topic)
	if !exist {
		return true
	}
	info := v.(*kernel.TopicPublishInfo)
	return info.MqList == nil || len(info.MqList) == 0
}

func (p *defaultProducer) IsUnitMode() bool {
	return false
}

func propertiesToString(properties map[string]string) string {
	if properties == nil {
		return ""
	}
	var str string
	for k, v := range properties {
		str += fmt.Sprintf("%s%v%s%v", k, byte(1), v, byte(2))
	}
	return str
}
