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

//#cgo LDFLAGS: -L/usr/local/lib/ -lrocketmq
//#include "rocketmq/CMessage.h"
//#include "rocketmq/CMessageExt.h"
import "C"
import "fmt"

type Message struct {
	Topic string
	Keys  string
	// improve: maybe []byte is better.
	Body string
}

func (msg *Message) String() string {
	return fmt.Sprintf("[topic: %s, keys: %s, body: %s]", msg.Topic, msg.Keys, msg.Body)
}

type MessageExt struct {
	Message
	MessageID string
	Tags      string
	// improve: is there is a method convert c++ map to go variable?
	cmsgExt *C.struct_CMessageExt
	//Properties  string
}

func (msgExt *MessageExt) String() string {
	return fmt.Sprintf("[messageId: %s, %s, Tags: %s]", msgExt.MessageID, msgExt.Message, msgExt.Tags)
}

func (msgExt *MessageExt) GetProperty(key string) string {
	return C.GoString(C.GetMessageProperty(msgExt.cmsgExt, C.CString(key)))
}

func cmsgToGo(cmsg *C.struct_CMessage) *Message {
	defer C.DestroyMessage(cmsg)
	gomsg := &Message{}

	return gomsg
}

func goMsgToC(gomsg *Message) *C.struct_CMessage {
	var cmsg = C.CreateMessage(C.CString(gomsg.Topic))

	// int(C.SetMessageKeys(msg.(*C.struct_CMessage),C.CString(keys)))
	C.SetMessageKeys(cmsg, C.CString(gomsg.Keys))

	// int(C.SetMessageBody(msg.(*C.struct_CMessage),C.CString(body)))
	C.SetMessageBody(cmsg, C.CString(gomsg.Body))
	return cmsg
}

//
func cmsgExtToGo(cmsg *C.struct_CMessageExt) *MessageExt {
	//defer C.DestroyMessageExt(cmsg)
	gomsg := &MessageExt{}

	gomsg.Topic = C.GoString(C.GetMessageTopic(cmsg))
	gomsg.Body = C.GoString(C.GetMessageBody(cmsg))
	gomsg.Keys = C.GoString(C.GetMessageKeys(cmsg))
	gomsg.Tags = C.GoString(C.GetMessageTags(cmsg))
	gomsg.MessageID = C.GoString(C.GetMessageId(cmsg))

	return gomsg
}

//
//func goMsgExtToC(gomsg *MessageExt) *C.struct_CMessageExt {
//	var cmsg = C.CreateMessage(C.CString(gomsg.Topic))
//
//	// int(C.SetMessageKeys(msg.(*C.struct_CMessage),C.CString(keys)))
//	C.SetMessageKeys(cmsg, C.CString(gomsg.Keys))
//
//	// int(C.SetMessageBody(msg.(*C.struct_CMessage),C.CString(body)))
//	C.SetMessageBody(cmsg, C.CString(gomsg.Body))
//	return cmsg
//}
