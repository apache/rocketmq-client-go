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

package main

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2/errors"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

func main() {
	c, err := rocketmq.NewPullConsumer(
		consumer.WithGroupName("testGroup"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
	)
	if err != nil {
		rlog.Error("Fail to New Pull Consumer", map[string]interface{}{
			rlog.LogKeyUnderlayError: err.Error(),
		})
	}
	err = c.Start()
	if err != nil {
		rlog.Error("Start Consumer Error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err.Error(),
		})
	}

	ctx := context.Background()
	queue := primitive.MessageQueue{
		Topic:      "TopicTest",
		BrokerName: "", // replace with your broker name. otherwise, pull will failed.
		QueueId:    0,
	}

	offset := int64(0)
	for {
		resp, err := c.PullFrom(ctx, queue, offset, 10)
		if err != nil {
			if err == errors.ErrRequestTimeout {
				rlog.Error("Pull Consumer Timeout", nil)
				time.Sleep(1 * time.Second)
				continue
			}
			rlog.Error("Pull Consumer Error", map[string]interface{}{
				rlog.LogKeyUnderlayError: err.Error(),
			})
			return
		}
		if resp.Status == primitive.PullFound {
			rlog.Info("Pull Message Success", map[string]interface{}{
				"nextOffset": resp.NextBeginOffset,
			})
			for _, msg := range resp.GetMessageExts() {
				rlog.Info("Pull Message", map[string]interface{}{
					"result": msg,
				})
			}
		}
		offset = resp.NextBeginOffset
	}
}
