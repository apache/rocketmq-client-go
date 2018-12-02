/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package rocketmq

/*
#cgo LDFLAGS: -L/usr/local/lib -lrocketmq

#include <stdio.h>
#include <stdlib.h>
#include "rocketmq/CMessageExt.h"
#include "rocketmq/CPullConsumer.h"
*/
import "C"

import (
	"fmt"
	"sync"
	"unsafe"
)

// DefaultPullConsumer default consumer pulling the message
type DefaultPullConsumer struct {
	config    *ConsumerConfig
	cconsumer *C.struct_CPullConsumer
	funcsMap  sync.Map

	resources []*C.char
}

func (c *DefaultPullConsumer) String() string {
	topics := ""
	c.funcsMap.Range(func(key, value interface{}) bool {
		topics += key.(string) + ", "
		return true
	})
	return fmt.Sprintf("[%s, subcribed topics: [%s]]", c.config, topics)
}

// Start starts the pulling conumser
func (c *DefaultPullConsumer) Start() error {
	C.StartPullConsumer(c.cconsumer)
	return nil
}

// Shutdown shutdown the pulling conumser
func (c *DefaultPullConsumer) Shutdown() error {
	C.ShutdownPullConsumer(c.cconsumer)
	C.DestroyPullConsumer(c.cconsumer)
	for _, r := range c.resources {
		C.free(unsafe.Pointer(r))
	}
	return nil
}

func (c *DefaultPullConsumer) FetchSubscriptionMessageQueues(topic string) []MessageQueue {

}

/*

CPullConsumer *CreatePullConsumer(const char *groupId);
int DestroyPullConsumer(CPullConsumer *consumer);
int StartPullConsumer(CPullConsumer *consumer);
int ShutdownPullConsumer(CPullConsumer *consumer);
int SetPullConsumerGroupID(CPullConsumer *consumer, const char *groupId);
const char *GetPullConsumerGroupID(CPullConsumer *consumer);
int SetPullConsumerNameServerAddress(CPullConsumer *consumer, const char *namesrv);
int SetPullConsumerSessionCredentials(CPullConsumer *consumer, const char *accessKey, const char *secretKey,
                                     const char *channel);
int SetPullConsumerLogPath(CPullConsumer *consumer, const char *logPath);
int SetPullConsumerLogFileNumAndSize(CPullConsumer *consumer, int fileNum, long fileSize);
int SetPullConsumerLogLevel(CPullConsumer *consumer, CLogLevel level);

int FetchSubscriptionMessageQueues(CPullConsumer *consumer, const char *topic, CMessageQueue **mqs , int* size);
int ReleaseSubscriptionMessageQueue(CMessageQueue *mqs);

CPullResult Pull(CPullConsumer *consumer,const CMessageQueue *mq, const char *subExpression, long long offset, int maxNums);
int ReleasePullResult(CPullResult pullResult);

*/
