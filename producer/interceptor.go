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

package producer

import "context"

// Invoker finish a send invoke on producer.
type Invoker func(ctx context.Context, req, reply interface{}) error

// Interceptor intercepts the execution of a send invoke on producer.
type Interceptor func(ctx context.Context, req, reply interface{}, next Invoker) error

// RetryInterceptor retry when send failed.
func RetryInterceptor() Interceptor {
	return func(ctx context.Context, req, reply interface{}, next Invoker) error {
		return nil
	}
}

// TimeoutInterceptor add a timeout listener in case of operation timeout.
func TimeoutInterceptor() Interceptor {
	return func(ctx context.Context, req, reply interface{}, next Invoker) error {
		return nil
	}
}

// LogInterceptor log a send invoke.
func LogInterceptor() Interceptor {
	return func(ctx context.Context, req, reply interface{}, next Invoker) error {
		return nil
	}
}
