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
#include <rocketmq/CCommon.h>
#include <rocketmq/CErrorMessage.h>
*/
import "C"
import "fmt"

type rmqError int

//This is error messages
const (
	NIL                        = rmqError(C.OK)
	ErrNullPoint               = rmqError(C.NULL_POINTER)
	ErrMallocFailed            = rmqError(C.MALLOC_FAILED)
	ErrProducerStartFailed     = rmqError(C.PRODUCER_START_FAILED)
	ErrSendSyncFailed          = rmqError(C.PRODUCER_SEND_SYNC_FAILED)
	ErrSendOnewayFailed        = rmqError(C.PRODUCER_SEND_ONEWAY_FAILED)
	ErrSendOrderlyFailed       = rmqError(C.PRODUCER_SEND_ORDERLY_FAILED)
	ErrSendTransactionFailed   = rmqError(C.PRODUCER_SEND_TRANSACTION_FAILED)
	ErrPushConsumerStartFailed = rmqError(C.PUSHCONSUMER_START_FAILED)
	ErrPullConsumerStartFailed = rmqError(C.PULLCONSUMER_START_FAILED)
	ErrFetchMQFailed           = rmqError(C.PULLCONSUMER_FETCH_MQ_FAILED)
	ErrFetchMessageFailed      = rmqError(C.PULLCONSUMER_FETCH_MESSAGE_FAILED)
	ErrNotSupportNow           = rmqError(C.NOT_SUPPORT_NOW)
)

func (e rmqError) Error() string {
	switch e {
	case ErrNullPoint:
		return "null point"
	case ErrMallocFailed:
		return "malloc memory failed"
	case ErrProducerStartFailed:
		return "start producer failed"
	case ErrSendSyncFailed:
		return "send message with sync failed"
	case ErrSendOrderlyFailed:
		return "send message with orderly failed"
	case ErrSendOnewayFailed:
		return "send message with oneway failed"
	case ErrSendTransactionFailed:
		return "send transaction message failed"
	case ErrPushConsumerStartFailed:
		return "start push-consumer failed"
	case ErrPullConsumerStartFailed:
		return "start pull-consumer failed"
	case ErrFetchMQFailed:
		return "fetch MessageQueue failed"
	case ErrFetchMessageFailed:
		return "fetch Message failed"
	case ErrNotSupportNow:
		return "this function is not support"
	default:
		return fmt.Sprintf("unknow error: %v", int(e))
	}
}

// GetLatestErrorMessage Get latest detailed error message from CPP-SDK
func GetLatestErrorMessage() string {
	return C.GoString(C.GetLatestErrorMessage())
}