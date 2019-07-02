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

import "github.com/apache/rocketmq-client-go/kernel"

type Option struct {
	apply func(*Options)
}

func NewOption(f func(options *Options)) *Option {
	return &Option{
		apply: f,
	}
}

type Options struct {
	interceptors []Interceptor

	kernel.ClientOption
	NameServerAddr           string
	GroupName                string
	RetryTimesWhenSendFailed int
	UnitMode                 bool
}

func defaultOptions() Options {
	return Options{
		RetryTimesWhenSendFailed:  2,
	}
}


func WithInterceptor(f Interceptor) *Option {
	return NewOption(func(options *Options) {
		options.interceptors = append(options.interceptors, f)
	})
}

func WithChainInterceptor(fs ...Interceptor) *Option {
	return NewOption(func(options *Options) {
		options.interceptors = append(options.interceptors, fs...)
	})
}

func WithRetry(retries int) *Option {
	return  NewOption(func(options *Options) {
		options.RetryTimesWhenSendFailed = retries
	})
}
