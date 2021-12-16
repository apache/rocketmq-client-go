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
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"os"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

// use concurrent consumer model, when Subscribe function return consumer.ConsumeRetryLater, the message will be
// send to RocketMQ retry topic. we could set DelayLevelWhenNextConsume in ConsumeConcurrentlyContext, which used to
// indicate the delay of message re-send to origin topic from retry topic.
//
// in this example, we always set DelayLevelWhenNextConsume=1, means that the message will be sent to origin topic after
// 1s. in case of the unlimited retry, we will return consumer.ConsumeSuccess after ReconsumeTimes > 5
func main() {
	c, _ := rocketmq.NewPushConsumer(
		consumer.WithGroupName("testGroup"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		consumer.WithConsumerModel(consumer.Clustering),
	)

	// The DelayLevel specify the waiting time that before next reconsume,
	// and it range is from 1 to 18 now.
	//
	// The time of each level is the value of indexing of {level-1} in [1s, 5s, 10s, 30s,
	// 1m, 2m, 3m, 4m, 5m, 6m, 7m, 8m, 9m, 10m, 20m, 30m, 1h, 2h]
	delayLevel := 1
	err := c.Subscribe("TopicTest", consumer.MessageSelector{}, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		rlog.Info("Subscribe Callback", map[string]interface{}{
			"msgs": msgs,
		})

		concurrentCtx, _ := primitive.GetConcurrentlyCtx(ctx)
		concurrentCtx.DelayLevelWhenNextConsume = delayLevel // only run when return consumer.ConsumeRetryLater

		for _, msg := range msgs {
			if msg.ReconsumeTimes > 5 {
				rlog.Info("Message Reconsume Times > 5", map[string]interface{}{
					"msg": msg,
				})
				return consumer.ConsumeSuccess, nil
			} else {
				rlog.Info("Subscribe Callback", map[string]interface{}{
					"msg": msg,
				})
			}
		}
		return consumer.ConsumeRetryLater, nil
	})
	if err != nil {
		rlog.Error("Subscribe Error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err.Error(),
		})
	}
	// Note: start after subscribe
	err = c.Start()
	if err != nil {
		rlog.Error("Start Consumer Error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err.Error(),
		})
		os.Exit(-1)
	}
	time.Sleep(time.Hour)
	err = c.Shutdown()
	if err != nil {
		rlog.Error("Shutdown Consumer Error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err.Error(),
		})
	}
}
