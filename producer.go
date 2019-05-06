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

package rocketmq

import (
	"context"
	"encoding/json"
	"github.com/apache/rocketmq-client-go/common"
	"github.com/apache/rocketmq-client-go/kernel"
	"github.com/apache/rocketmq-client-go/rlog"
	"github.com/apache/rocketmq-client-go/utils"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Producer interface {
	Start()
	Send(msg *kernel.Message) (*kernel.SendResult, error)
}

type defaultProducer struct {
	state  kernel.ServiceState
	config ProducerConfig
}

type ProducerConfig struct {
	GroupName                  string
	CommunicationType          int
	RetryTimesWhenSendFailed   int
}

func (c *defaultProducer) Send(msg *kernel.Message) (*kernel.SendResult, error) {
	result :=tryToFindTopicPublishInfo(msg.Topic)
	if result != nil && result.HaveTopicRouterInfo {
		if c.config.RetryTimesWhenSendFailed == 0 {
			c.config.RetryTimesWhenSendFailed = 2
		}
		retryTime := 1
		if  c.config.CommunicationType == common.SYNC {
			retryTime = 1 + c.config.RetryTimesWhenSendFailed
		}
		for retryCount := 0;  retryCount < retryTime; retryCount++ {
			messageQueue := getMessageQueue(result)
			if messageQueue == nil {
				continue
			}

			brokerAddress := kernel.FindBrokerAddressInPublish(messageQueue.BrokerName)
			if brokerAddress == "" {
				break
			}
			brokerAddress = brokerVIPChannel(brokerAddress)
			sysFlag := 0
			tranMsg := msg.Properties["TRAN_MSG"]
			if tranMsg != "" {
				tranMsgBool, err :=strconv.ParseBool(tranMsg)
				if err == nil && tranMsgBool {
					sysFlag |= utils.TRANSACTION_PREPARED_TYPE;
				}
				sendMsg, err := getSendMessage(msg, c, messageQueue, sysFlag)
				if err != nil {
					continue
				}
				return sendKernel(brokerAddress, c, msg, sendMsg, messageQueue)
			}
		}
	}
	return nil, nil
}

func sendKernel(brokerAddress string, receive *defaultProducer, msg *kernel.Message, sendMsg *kernel.SendMessageRequest, messageQueue *kernel.MessageQueue) (*kernel.SendResult, error) {
	switch receive.config.CommunicationType{
	case common.SYNC:
		return kernel.SendMessageSync(context.Background(), brokerAddress, messageQueue.BrokerName, sendMsg, []*kernel.Message{msg})
	case common.ASYNC:
		//kernel.SendMessageAsync()
		return nil,nil
	case common.ONEWAY:
		return kernel.SendMessageOneWay(context.Background(), brokerAddress, sendMsg, []*kernel.Message{msg})
	default:
		return nil,nil
	}
}

func getSendMessage(msg *kernel.Message, receiver *defaultProducer, messageQueue *kernel.MessageQueue, sysFlag int) (*kernel.SendMessageRequest,error) {

	properties, err := json.Marshal(msg.Properties)
	maxReconsumeTimes, errtimes := strconv.Atoi(msg.Properties["MAX_RECONSUME_TIMES"])
	if errtimes != nil {
		maxReconsumeTimes = 1
	}
	if err == nil {
		sendMessageRequest := &kernel.SendMessageRequest{
			ProducerGroup:         receiver.config.GroupName,
			Topic:                 msg.Topic,
			DefaultTopic:          "TBW102",
			DefaultTopicQueueNums: 8,
			QueueId:               int32(messageQueue.QueueId),
			SysFlag:			   sysFlag,
			BornTimestamp:         time.Now().Unix(),
			Flag:				   int(msg.Flag),
			Properties:            string(properties),
			ReconsumeTimes:		   0,
			UnitMode:			   false,
			MaxReconsumeTimes:     maxReconsumeTimes,
			Batch:                 msg.Batch,
		}
		return sendMessageRequest,nil
	}
	return nil,err
}


func brokerVIPChannel(brokerAddr string) string {
	var brokerAddrNew strings.Builder
	var isChange bool
	var err error
	if os.Getenv("com.rocketmq.sendMessageWithVIPChannel") != "" {
		isChange, err = strconv.ParseBool(os.Getenv("com.rocketmq.sendMessageWithVIPChannel"))
		if err != nil {
			isChange = true
		}
	}else{
		isChange = true
	}
	if isChange {
	ipAndPort := strings.Split(brokerAddr, ":")

	port, err :=strconv.Atoi(ipAndPort[1])

	if err != nil {
		return ""
	}
	brokerAddrNew.WriteString(ipAndPort[0])
	brokerAddrNew.WriteString(":")
	brokerAddrNew.WriteString(strconv.Itoa(port-2))
	return brokerAddrNew.String();
	} else {
		return brokerAddr;
	}
}


func tryToFindTopicPublishInfo(topic string) *kernel.TopicPublishInfo {
	result := kernel.FindTopicPublishInfo(topic)

	if result == nil {
		kernel.UpdateTopicRouteInfo(topic)
	}
	return kernel.FindTopicPublishInfo(topic)
}

func getMessageQueue(tpInfo *kernel.TopicPublishInfo) *kernel.MessageQueue {
	if tpInfo.MqList != nil && len(tpInfo.MqList) <= 0 {
		rlog.Error("can not find proper message queue")
		return nil
	}
	return tpInfo.MqList[int(atomic.AddInt32(&tpInfo.TopicQueueIndex, 1))%len(tpInfo.MqList)]
}

func (c *defaultProducer) Start() {
	c.state = kernel.Running
}

func NewProducer(config ProducerConfig) Producer {
	return &defaultProducer{
		config: config,
	}
}

