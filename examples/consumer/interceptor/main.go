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
	"os"
	"time"

	"github.com/apache/rocketmq-client-go/internal/consumer"
	"github.com/apache/rocketmq-client-go/primitive"
)

func main() {
	c, _ := consumer.NewPushConsumer("testGroup", []string{"127.0.0.1:9876"},
		primitive.WithConsumerModel(primitive.Clustering),
		primitive.WithConsumeFromWhere(primitive.ConsumeFromFirstOffset),
		primitive.WithChainConsumerInterceptor(UserFistInterceptor(), UserSecondInterceptor()))
	err := c.Subscribe("TopicTest", primitive.MessageSelector{}, func(ctx *primitive.ConsumeMessageContext,
		msgs []*primitive.MessageExt) (primitive.ConsumeResult, error) {
		fmt.Println("subscribe callback: %v", msgs)
		return primitive.ConsumeSuccess, nil
	})
	if err != nil {
		fmt.Println(err.Error())
	}
	// Note: start after subscribe
	err = c.Start()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	time.Sleep(time.Hour)
}

func UserFistInterceptor() primitive.CInterceptor {
	return func(ctx context.Context, req, reply interface{}, next primitive.CInvoker) error {
		msgCtx, _ := primitive.GetConsumerCtx(ctx)
		fmt.Printf("msgCtx: %v, mehtod: %s", msgCtx, primitive.GetMethod(ctx))

		msgs := req.([]*primitive.MessageExt)
		fmt.Printf("user first interceptor before invoke: %v\n", msgs)
		e := next(ctx, msgs, reply)

		holder := reply.(*primitive.ConsumeResultHolder)
		fmt.Printf("user first interceptor after invoke: %v, result: %v\n", msgs, holder)
		return e
	}
}

func UserSecondInterceptor() primitive.CInterceptor {
	return func(ctx context.Context, req, reply interface{}, next primitive.CInvoker)  error {
		msgs := req.([]*primitive.MessageExt)
		fmt.Printf("user second interceptor before invoke: %v\n", msgs)
		e := next(ctx, msgs, reply)
		holder := reply.(*primitive.ConsumeResultHolder)
		fmt.Printf("user second interceptor after invoke: %v, result: %v\n", msgs, holder)
		return e
	}
}
