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

// Package main implements a producer with user custom interceptor.
package main

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"os"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

func main() {
	p, err := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		producer.WithRetry(2),
		producer.WithCredentials(primitive.Credentials{
			AccessKey: "RocketMQ",
			SecretKey: "12345678",
		}),
	)

	if err != nil {
		rlog.Error("Init Producer Error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err.Error(),
		})
		os.Exit(0)
	}

	err = p.Start()
	if err != nil {
		rlog.Error("Start Producer Error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err.Error(),
		})
		os.Exit(1)
	}
	for i := 0; i < 100000; i++ {
		res, err := p.SendSync(context.Background(), primitive.NewMessage("test",
			[]byte("Hello RocketMQ Go Client!")))

		if err != nil {
			rlog.Error("Send Message Error", map[string]interface{}{
				rlog.LogKeyUnderlayError: err.Error(),
			})
		} else {
			rlog.Info("Send Message Success", map[string]interface{}{
				"result": res.String(),
			})
		}
	}
	err = p.Shutdown()
	if err != nil {
		rlog.Error("Shutdown Producer Error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err.Error(),
		})
	}
}
