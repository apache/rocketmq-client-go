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
#cgo LDFLAGS: -L/usr/local/lib -lrocketmq

#include <rocketmq/CMessage.h>
#include <rocketmq/CMessageExt.h>
#include <rocketmq/CProducer.h>
*/
import "C"
import "sync"

var transactionProducerMap sync.Map

//export localTransactionExecutorCallback
func localTransactionExecutorCallback(cproducer *C.CProducer, msg *C.CMessage, arg interface{}) C.int {
	producer, exist := transactionProducerMap.Load(cproducer)
	if !exist {
		return C.int(UnknownTransaction)
	}

	message := cMsgToGo(msg)
	listenerWrap, exist := producer.(*defaultTransactionProducer).listenerFuncsMap.Load(cproducer)
	if !exist {
		return C.int(UnknownTransaction)
	}
	status := listenerWrap.(TransactionLocalListener).Execute(message, arg)
	return C.int(status)
}

//export localTransactionCheckerCallback
func localTransactionCheckerCallback(cproducer *C.CProducer, msg *C.CMessageExt, arg interface{}) C.int {
	producer, exist := transactionProducerMap.Load(cproducer)
	if !exist {
		return C.int(UnknownTransaction)
	}

	message := cmsgExtToGo(msg)
	listener, exist := producer.(*defaultTransactionProducer).listenerFuncsMap.Load(cproducer)
	if !exist {
		return C.int(UnknownTransaction)
	}
	status := listener.(TransactionLocalListener).Check(message, arg)
	return C.int(status)
}
