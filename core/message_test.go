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

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMessage_String(t *testing.T) {
	msg := Message{
		Topic:          "testTopic",
		Tags:           "TagA, TagB",
		Keys:           "Key1, Key2",
		Body:           "Body1234567890",
		DelayTimeLevel: 8}
	expect := "[Topic: testTopic, Tags: TagA, TagB, Keys: Key1, Key2, Body: Body1234567890, DelayTimeLevel: 8," +
		" Property: map[]]"
	assert.Equal(t, expect, msg.String())
}

func TestMessage_GetProperty(t *testing.T) {
	msg := Message{
		Topic:          "testTopic",
		Tags:           "TagA, TagB",
		Keys:           "Key1, Key2",
		Body:           "Body1234567890",
		DelayTimeLevel: 8}
	cmsg := goMsgToC(&msg)
	newMsg := cMsgToGo(cmsg)
	expect := "[Topic: testTopic, Tags: TagA, TagB, Keys: Key1, Key2, Body: Body1234567890, DelayTimeLevel: 8," +
		" Property: map[]]"
	assert.Equal(t, expect, newMsg.String())
	val := newMsg.GetProperty("KEY")
	assert.Empty(t, val)
}

func TestMessageExt_String(t *testing.T) {
	msg := Message{
		Topic:          "testTopic",
		Tags:           "TagA, TagB",
		Keys:           "Key1, Key2",
		Body:           "Body1234567890",
		DelayTimeLevel: 8}
	msgExt := MessageExt{
		Message:                   msg,
		MessageID:                 "messageId",
		QueueId:                   2,
		ReconsumeTimes:            13,
		StoreSize:                 1 << 10,
		BornTimestamp:             int64(1234567890897),
		StoreTimestamp:            int64(1234567890),
		QueueOffset:               int64(1234567890),
		CommitLogOffset:           int64(1234567890),
		PreparedTransactionOffset: int64(1234567890),
	}
	expect := "[MessageId: messageId, [Topic: testTopic, Tags: TagA, TagB, Keys: Key1, Key2, " +
		"Body: Body1234567890, DelayTimeLevel: 8, Property: map[]], QueueId: 2, ReconsumeTimes: " +
		"13, StoreSize: 1024, BornTimestamp: 1234567890897, StoreTimestamp: 1234567890, QueueOffset: 1234567890," +
		" CommitLogOffset: 1234567890, PreparedTransactionOffset: 1234567890]"
	assert.Equal(t, expect, msgExt.String())
}
