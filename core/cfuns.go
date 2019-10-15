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

#include <rocketmq/CMessageExt.h>
#include <rocketmq/CPushConsumer.h>
*/
import "C"
import (
	"sync"
)

var pushConsumerMap sync.Map

//export consumeMessageCallback
func consumeMessageCallback(cconsumer *C.CPushConsumer, msg *C.CMessageExt) C.int {
	consumer, exist := pushConsumerMap.Load(cconsumer)
	if !exist {
		return C.int(ReConsumeLater)
	}

	msgExt := cmsgExtToGo(msg)
	//C.DestroyMessageExt(msg)
	cfunc, exist := consumer.(*defaultPushConsumer).funcsMap.Load(msgExt.Topic)
	if !exist {
		return C.int(ReConsumeLater)
	}
	return C.int(cfunc.(func(msg *MessageExt) ConsumeStatus)(msgExt))
}
