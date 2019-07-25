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

package primitive

import (
	"context"
)

// Invoker finish a message invoke on producer/consumer.
type Invoker func(ctx context.Context, req, reply interface{}) error

// Interceptor intercepts the invoke of a producer/consumer on messages.
// In PushConsumer call, the req is []*MessageExt type and the reply is ConsumeResultHolder,
// use type assert to get real type.
type Interceptor func(ctx context.Context, req, reply interface{}, next Invoker) error

// config for message trace.
type TraceConfig struct {
	TraceTopic string
	Access     AccessChannel
}
