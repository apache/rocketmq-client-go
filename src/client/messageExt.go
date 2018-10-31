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
package client

//#cgo CFLAGS: -I/usr/local/include/rocketmq
//#cgo LDFLAGS: -L/usr/local/lib -lrocketmq
//typedef struct CMessageExt CMessageExt;
//#include "CMessageExt.h"
import "C"

//type MessageExt C.struct_CMessageExt

func GetMessageTopic(msg MessageExt)(topic string){
    topic = C.GoString(C.GetMessageTopic(msg.(*C.struct_CMessageExt)))
    return
}
func GetMessageTags(msg MessageExt)(tags string){
	tags = C.GoString(C.GetMessageTags(msg.(*C.struct_CMessageExt)))
	return
}
func GetMessageKeys(msg MessageExt)(keys string){
	keys = C.GoString(C.GetMessageKeys(msg.(*C.struct_CMessageExt)))
	return
}
func GetMessageBody(msg MessageExt)(body string){
	body = C.GoString(C.GetMessageBody(msg.(*C.struct_CMessageExt)))
	return
}
func GetMessageProperty(msg MessageExt,key string)(value string){
	value = C.GoString(C.GetMessageProperty(msg.(*C.struct_CMessageExt),C.CString(key)))
	return
}
func GetMessageId(msg MessageExt)(msgId string){
	msgId = C.GoString(C.GetMessageId(msg.(*C.struct_CMessageExt)))
	return
}