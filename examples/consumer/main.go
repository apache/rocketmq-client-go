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
	"fmt"
	"github.com/apache/rocketmq-client-go/consumer"
	"github.com/apache/rocketmq-client-go/kernel"
	"os"
	"time"
)

func main() {
	c := consumer.NewPushConsumer("testGroup", consumer.ConsumerOption{
		NameServerAddr: "127.0.0.1:9876",
		ConsumerModel:  consumer.Clustering,
		FromWhere:      consumer.ConsumeFromFirstOffset,
	})
	//var count int64
	err := c.Subscribe("test", consumer.MessageSelector{}, func(ctx *consumer.ConsumeMessageContext,
		msgs []*kernel.MessageExt) (consumer.ConsumeResult, error) {
		//c := atomic.AddInt64(&count, int64(len(msgs)))
		//if c%1000 == 0 {
		fmt.Println(msgs)
		//}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		fmt.Println(err.Error())
	}
	err = c.Start()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	time.Sleep(time.Hour)
}
