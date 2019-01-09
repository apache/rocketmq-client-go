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
#cgo LDFLAGS: -L/usr/local/lib/ -lrocketmq
#include <rocketmq/CMessage.h>
#include <rocketmq/CMessageExt.h>
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"unsafe"
)

type Message struct {
	Topic          string
	Tags           string
	Keys           string
	Body           string
	DelayTimeLevel int
	Property       map[string]string
}

func (msg *Message) String() string {
	return fmt.Sprintf("[Topic: %s, Tags: %s, Keys: %s, Body: %s, DelayTimeLevel: %d, Property: %v]",
		msg.Topic, msg.Tags, msg.Keys, msg.Body, msg.DelayTimeLevel, msg.Property)
}

func goMsgToC(gomsg *Message) *C.struct_CMessage {
	cs := C.CString(gomsg.Topic)
	var cmsg = C.CreateMessage(cs)
	C.free(unsafe.Pointer(cs))

	cs = C.CString(gomsg.Tags)
	C.SetMessageTags(cmsg, cs)
	C.free(unsafe.Pointer(cs))

	cs = C.CString(gomsg.Keys)
	C.SetMessageKeys(cmsg, cs)
	C.free(unsafe.Pointer(cs))

	cs = C.CString(gomsg.Body)
	C.SetMessageBody(cmsg, cs)
	C.free(unsafe.Pointer(cs))

	C.SetDelayTimeLevel(cmsg, C.int(gomsg.DelayTimeLevel))

	for k, v := range gomsg.Property {
		key := C.CString(k)
		value := C.CString(v)
		C.SetMessageProperty(cmsg, key, value)
		C.free(unsafe.Pointer(key))
		C.free(unsafe.Pointer(value))
	}
	return cmsg
}

type MessageExt struct {
	Message
	MessageID                 string
	QueueId                   int
	ReconsumeTimes            int
	StoreSize                 int
	BornTimestamp             int64
	StoreTimestamp            int64
	QueueOffset               int64
	CommitLogOffset           int64
	PreparedTransactionOffset int64

	// improve: is there is a method convert c++ map to go variable?
	cmsgExt *C.struct_CMessageExt
}

func (msgExt *MessageExt) String() string {
	return fmt.Sprintf("[MessageId: %s, %s, QueueId: %d, ReconsumeTimes: %d, StoreSize: %d, BornTimestamp: %d, "+
		"StoreTimestamp: %d, QueueOffset: %d, CommitLogOffset: %d, PreparedTransactionOffset: %d]", msgExt.MessageID,
		msgExt.Message.String(), msgExt.QueueId, msgExt.ReconsumeTimes, msgExt.StoreSize, msgExt.BornTimestamp,
		msgExt.StoreTimestamp, msgExt.QueueOffset, msgExt.CommitLogOffset, msgExt.PreparedTransactionOffset)
}

func (msgExt *MessageExt) GetProperty(key string) string {
	return C.GoString(C.GetMessageProperty(msgExt.cmsgExt, C.CString(key)))
}

func cmsgExtToGo(cmsg *C.struct_CMessageExt) *MessageExt {
	gomsg := &MessageExt{cmsgExt: cmsg}

	gomsg.Topic = C.GoString(C.GetMessageTopic(cmsg))
	gomsg.Tags = C.GoString(C.GetMessageTags(cmsg))
	gomsg.Keys = C.GoString(C.GetMessageKeys(cmsg))
	gomsg.Body = C.GoString(C.GetMessageBody(cmsg))
	gomsg.MessageID = C.GoString(C.GetMessageId(cmsg))
	gomsg.DelayTimeLevel = int(C.GetMessageDelayTimeLevel(cmsg))
	gomsg.QueueId = int(C.GetMessageQueueId(cmsg))
	gomsg.ReconsumeTimes = int(C.GetMessageReconsumeTimes(cmsg))
	gomsg.StoreSize = int(C.GetMessageStoreSize(cmsg))
	gomsg.BornTimestamp = int64(C.GetMessageBornTimestamp(cmsg))
	gomsg.StoreTimestamp = int64(C.GetMessageStoreTimestamp(cmsg))
	gomsg.QueueOffset = int64(C.GetMessageQueueOffset(cmsg))
	gomsg.CommitLogOffset = int64(C.GetMessageCommitLogOffset(cmsg))
	gomsg.PreparedTransactionOffset = int64(C.GetMessagePreparedTransactionOffset(cmsg))

	return gomsg
}
