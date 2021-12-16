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

func main() {
	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName("testGroup"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		consumer.WithCredentials(primitive.Credentials{
			AccessKey: "RocketMQ",
			SecretKey: "12345678",
		}),
	)
	if err != nil {
		rlog.Error("Init Consumer Error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err.Error(),
		})
		os.Exit(0)
	}

	err = c.Subscribe("test", consumer.MessageSelector{}, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		rlog.Info("Subscribe Callback", map[string]interface{}{
			"msgs": msgs,
		})
		return consumer.ConsumeSuccess, nil
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
