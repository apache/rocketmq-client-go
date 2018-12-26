/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package rocketmq

/*
#include "rocketmq/CCommon.h"
*/
import "C"

type RMQError int

const (
	NullPoint               = RMQError(C.NULL_POINTER)
	MallocFailed            = RMQError(C.MALLOC_FAILED)
	ProducerStartFailed     = RMQError(C.PRODUCER_START_FAILED)
	SendSyncFailed          = RMQError(C.PRODUCER_SEND_SYNC_FAILED)
	SendOnewayFailed        = RMQError(C.PRODUCER_SEND_ONEWAY_FAILED)
	SendOrderlyFailed       = RMQError(C.PRODUCER_SEND_ORDERLY_FAILED)
	PushConsumerStartFailed = RMQError(C.PUSHCONSUMER_ERROR_CODE_START)
	PullConsumerStartFailed = RMQError(C.PULLCONSUMER_START_FAILED)
	FetchMQFailed           = RMQError(C.PULLCONSUMER_FETCH_MQ_FAILED)
	FetchMessageFailed      = RMQError(C.PULLCONSUMER_FETCH_MESSAGE_FAILED)
)

func (e RMQError) Error() string {
	switch e {
	case NullPoint:
		return "null point"
	case MallocFailed:
		return "malloc memory failed"
	case ProducerStartFailed:
		return "start producer failed"
	case SendSyncFailed:
		return "send message with sync failed"
	case SendOrderlyFailed:
		return "send message with orderly failed"
	case SendOnewayFailed:
		return "send message with oneway failed"
	case PushConsumerStartFailed:
		return "start push-consumer failed"
	case PullConsumerStartFailed:
		return "start pull-consumer failed"
	case FetchMQFailed:
		return "fetch MessageQueue failed"
	case FetchMessageFailed:
		return "fetch Message failed"
	default:
		return "unknow error."
	}
}
