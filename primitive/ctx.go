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

/*
 * Define the ctx key and value type.
 */
package primitive

import "context"

type CtxKey int

const (
	method CtxKey = iota


	// method name in  producer
	SendSync = "SendSync"
	SendOneway = "SendOneway"
	// method name in consumer
	ConsumerPush = "ConsumerPush"
	ConsumerPull = "ConsumerPull"
)

// WithMehod set call method name
func WithMehod(ctx context.Context, m string) context.Context {
	return context.WithValue(ctx, method, m)
}

// GetMethod get call method name
func GetMethod(ctx context.Context) string {
	return ctx.Value(method).(string)
}

