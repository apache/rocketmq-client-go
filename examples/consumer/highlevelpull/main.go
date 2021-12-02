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
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/errors"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

func main() {
	c, err := rocketmq.NewHighLevelPullConsumer(
		consumer.WithGroupName("testGroup"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
	)

	if err != nil {
		rlog.Fatal(fmt.Sprintf("fail to new pullConsumer: %s", err), nil)
	}
	c.Subscribe("test", consumer.MessageSelector{})
	err = c.Start()
	if err != nil {
		rlog.Fatal(fmt.Sprintf("fail to new pullConsumer: %s", err), nil)
	}
	ctx := context.Background()
	for {
		resp, err := c.Pull(ctx, "test", 10)
		if err != nil {
			if err == errors.ErrRequestTimeout {
				fmt.Printf("timeout \n")
				//time.Sleep(1 * time.Second)
				continue
			}
			fmt.Printf("unexpectable err: %v \n", err)
			return
		}
		if resp.Status == primitive.PullFound {
			fmt.Printf("pull message success. nextOffset: %d \n", resp.NextBeginOffset)
			for _, msg := range resp.GetMessageExts() {
				fmt.Printf("pull msg: %v \n", msg)
			}
		}
	}
}