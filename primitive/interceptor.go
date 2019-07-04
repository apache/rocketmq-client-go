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

// PInvoker finish a send invoke on producer.
type PInvoker func(ctx context.Context, req, reply interface{}) error

// PInterceptor intercepts the execution of a send invoke on producer.
type PInterceptor func(ctx context.Context, req, reply interface{}, next PInvoker) error

// RetryInterceptor retry when send failed.
func RetryPInterceptor() PInterceptor {
	return func(ctx context.Context, req, reply interface{}, next PInvoker) error {
		return nil
	}
}

// TimeoutInterceptor add a timeout listener in case of operation timeout.
func TimeoutPInterceptor() PInterceptor {
	return func(ctx context.Context, req, reply interface{}, next PInvoker) error {
		return nil
	}
}

// LogInterceptor log a send invoke.
func LogPInterceptor() PInterceptor {
	return func(ctx context.Context, req, reply interface{}, next PInvoker) error {
		return nil
	}
}

// CInvoker finish a message invoke on consumer
type CInvoker func(ctx context.Context, msgs []*MessageExt, reply *ConsumeResultHolder) error

// CInterceptor intercepts the invoke of a consume on messages.
type CInterceptor func(ctx context.Context, msgs []*MessageExt, reply *ConsumeResultHolder, next CInvoker) error
