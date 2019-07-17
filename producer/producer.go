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
	"fmt"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/internal"
	"github.com/apache/rocketmq-client-go/internal/remote"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/apache/rocketmq-client-go/rlog"
	"github.com/pkg/errors"
)

var (
	ErrTopicEmpty   = errors.New("topic is nil")
	ErrMessageEmpty = errors.New("message is nil")
	ErrNotRunning   = errors.New("producer not started")
)

func NewDefaultProducer(opts ...Option) (*defaultProducer, error) {
	defaultOpts := defaultProducerOptions()
	for _, apply := range opts {
		apply(&defaultOpts)
	}
	srvs, err := internal.NewNamesrv(defaultOpts.NameServerAddrs...)
	if err != nil {
		return nil, errors.Wrap(err, "new Namesrv failed.")
	}
	internal.RegisterNamsrv(srvs)

	producer := &defaultProducer{
		group:   "default",
		client:  internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions),
		options: defaultOpts,
	}

	chainInterceptor(producer)

	return producer, nil
}

// chainInterceptor chain list of interceptor as one interceptor
func chainInterceptor(p *defaultProducer) {
	interceptors := p.options.Interceptors
	switch len(interceptors) {
	case 0:
		p.interceptor = nil
	case 1:
		p.interceptor = interceptors[0]
	default:
		p.interceptor = func(ctx context.Context, req, reply interface{}, invoker primitive.Invoker) error {
			return interceptors[0](ctx, req, reply, getChainedInterceptor(interceptors, 0, invoker))
		}
	}
}

// getChainedInterceptor recursively generate the chained invoker.
func getChainedInterceptor(interceptors []primitive.Interceptor, cur int, finalInvoker primitive.Invoker) primitive.Invoker {
	if cur == len(interceptors)-1 {
		return finalInvoker
	}
	return func(ctx context.Context, req, reply interface{}) error {
		return interceptors[cur+1](ctx, req, reply, getChainedInterceptor(interceptors, cur+1, finalInvoker))
	}
}

type defaultProducer struct {
	group       string
	client      internal.RMQClient
	state       internal.ServiceState
	options     producerOptions
	publishInfo sync.Map

	interceptor primitive.Interceptor
}

func (p *defaultProducer) Start() error {
	p.state = internal.StateRunning
	p.client.RegisterProducer(p.group, p)
	p.client.Start()
	return nil
}

func (p *defaultProducer) Shutdown() error {
	p.state = internal.StateShutdown
	p.client.Shutdown()
	return nil
}

func (p *defaultProducer) checkMsg(msg *primitive.Message) error {
	if p.state != internal.StateRunning {
		return ErrNotRunning
	}

	if msg == nil {
		return errors.New("message is nil")
	}

	if msg.Topic == "" {
		return errors.New("topic is nil")
	}
	return nil
}

func (p *defaultProducer) SendSync(ctx context.Context, msg *primitive.Message) (*primitive.SendResult, error) {
	if err := p.checkMsg(msg); err != nil {
		return nil, err
	}

	resp := new(primitive.SendResult)
	if p.interceptor != nil {
		primitive.WithMethod(ctx, primitive.SendSync)
		err := p.interceptor(ctx, msg, resp, func(ctx context.Context, req, reply interface{}) error {
			var err error
			realReq := req.(*primitive.Message)
			realReply := reply.(*primitive.SendResult)
			err = p.sendSync(ctx, realReq, realReply)
			return err
		})
		return resp, err
	}

	err := p.sendSync(ctx, msg, resp)
	return resp, err
}

func (p *defaultProducer) sendSync(ctx context.Context, msg *primitive.Message, resp *primitive.SendResult) error {

	retryTime := 1 + p.options.RetryTimes

	var (
		err error
	)

	for retryCount := 0; retryCount < retryTime; retryCount++ {
		mq := p.selectMessageQueue(msg)
		if mq == nil {
			err = fmt.Errorf("the topic=%s route info not found", msg.Topic)
			continue
		}

		addr := internal.FindBrokerAddrByName(mq.BrokerName)
		if addr == "" {
			return fmt.Errorf("topic=%s route info not found", mq.Topic)
		}

		res, _err := p.client.InvokeSync(addr, p.buildSendRequest(mq, msg), 3*time.Second)
		if _err != nil {
			err = _err
			continue
		}
		return p.client.ProcessSendResponse(mq.BrokerName, res, resp, msg)
	}
	return err
}

func (p *defaultProducer) SendAsync(ctx context.Context, f func(context.Context, *primitive.SendResult, error), msg *primitive.Message) error {
	if err := p.checkMsg(msg); err != nil {
		return err
	}

	if p.interceptor != nil {
		primitive.WithMethod(ctx, primitive.SendAsync)

		return p.interceptor(ctx, msg, nil, func(ctx context.Context, req, reply interface{}) error {
			return p.sendAsync(ctx, msg, f)
		})
	}
	return p.sendAsync(ctx, msg, f)
}

func (p *defaultProducer) sendAsync(ctx context.Context, msg *primitive.Message, h func(context.Context, *primitive.SendResult, error)) error {
	mq := p.selectMessageQueue(msg)
	if mq == nil {
		return errors.Errorf("the topic=%s route info not found", msg.Topic)
	}

	addr := internal.FindBrokerAddrByName(mq.BrokerName)
	if addr == "" {
		return errors.Errorf("topic=%s route info not found", mq.Topic)
	}

	return p.client.InvokeAsync(addr, p.buildSendRequest(mq, msg), 3*time.Second, func(command *remote.RemotingCommand, err error) {
		resp := new(primitive.SendResult)
		if err != nil {
			h(ctx, nil, err)
		} else {
			p.client.ProcessSendResponse(mq.BrokerName, command, resp, msg)
			h(ctx, resp, nil)
		}
	})
}

func (p *defaultProducer) SendOneWay(ctx context.Context, msg *primitive.Message) error {
	if err := p.checkMsg(msg); err != nil {
		return err
	}

	if p.interceptor != nil {
		primitive.WithMethod(ctx, primitive.SendOneway)
		return p.interceptor(ctx, msg, nil, func(ctx context.Context, req, reply interface{}) error {
			return p.SendOneWay(ctx, msg)
		})
	}

	return p.sendOneWay(ctx, msg)
}

func (p *defaultProducer) sendOneWay(ctx context.Context, msg *primitive.Message) error {
	retryTime := 1 + p.options.RetryTimes

	var (
		err error
	)
	for retryCount := 0; retryCount < retryTime; retryCount++ {
		mq := p.selectMessageQueue(msg)
		if mq == nil {
			err = fmt.Errorf("the topic=%s route info not found", msg.Topic)
			continue
		}

		addr := internal.FindBrokerAddrByName(mq.BrokerName)
		if addr == "" {
			return fmt.Errorf("topic=%s route info not found", mq.Topic)
		}

		_err := p.client.InvokeOneWay(addr, p.buildSendRequest(mq, msg), 3*time.Second)
		if _err != nil {
			err = _err
			continue
		}
		return nil
	}
	return err
}

func (p *defaultProducer) buildSendRequest(mq *primitive.MessageQueue,
	msg *primitive.Message) *remote.RemotingCommand {
	req := &internal.SendMessageRequest{
		ProducerGroup:  p.group,
		Topic:          mq.Topic,
		QueueId:        mq.QueueId,
		SysFlag:        0,
		BornTimestamp:  time.Now().UnixNano() / int64(time.Millisecond),
		Flag:           msg.Flag,
		Properties:     primitive.MarshalPropeties(msg.Properties),
		ReconsumeTimes: 0,
		UnitMode:       p.options.UnitMode,
		Batch:          false,
	}

	return remote.NewRemotingCommand(internal.ReqSendMessage, req, msg.Body)
}

func (p *defaultProducer) selectMessageQueue(msg *primitive.Message) *primitive.MessageQueue {
	topic := msg.Topic

	v, exist := p.publishInfo.Load(topic)
	if !exist {
		p.client.UpdatePublishInfo(topic, internal.UpdateTopicRouteInfo(topic))
		v, exist = p.publishInfo.Load(topic)
	}

	if !exist {
		return nil
	}

	result := v.(*internal.TopicPublishInfo)
	if result == nil || !result.HaveTopicRouterInfo {
		return nil
	}

	if result.MqList != nil && len(result.MqList) <= 0 {
		rlog.Error("can not find proper message queue")
		return nil
	}

	i := p.options.Selector.Select(msg, len(result.MqList))
	return result.MqList[i]
}

func (p *defaultProducer) PublishTopicList() []string {
	topics := make([]string, 0)
	p.publishInfo.Range(func(key, value interface{}) bool {
		topics = append(topics, key.(string))
		return true
	})
	return topics
}

func (p *defaultProducer) UpdateTopicPublishInfo(topic string, info *internal.TopicPublishInfo) {
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
	info := v.(*internal.TopicPublishInfo)
	return info.MqList == nil || len(info.MqList) == 0
}

func (p *defaultProducer) IsUnitMode() bool {
	return false
}
