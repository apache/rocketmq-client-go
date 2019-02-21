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
package remote

import (
	"math/rand"
	"reflect"
	"testing"
)

func randomBytes(length int) []byte {
	bs := make([]byte, length)
	if _, err := rand.Read(bs); err != nil {
		panic("read random bytes fail")
	}
	return bs
}

func randomString(length int) string {
	bs := make([]byte, length)
	for i := 0; i < len(bs); i++ {
		bs[i] = byte(97 + rand.Intn(26))
	}
	return string(bs)
}

func randomNewRemotingCommand() *RemotingCommand {
	properties := make(map[string]string)
	for i := 0; i < 10; i++ {
		properties[randomString(rand.Intn(20))] = randomString(rand.Intn(20))
	}
	body := randomBytes(rand.Intn(100))
	return NewRemotingCommand(int16(rand.Intn(1000)), properties, body)
}

func Test_encode(t *testing.T) {
	for i := 0; i < 1000; i++ {
		rc := randomNewRemotingCommand()
		if _, err := encode(rc); err != nil {
			t.Fatalf("encode RemotingCommand to bytes fail: %v", err)
		}
	}
}

func Benchmark_encode(b *testing.B) {
	rc := randomNewRemotingCommand()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := encode(rc); err != nil {
			b.Fatalf("encode RemotingCommand to bytes fail: %v", err)
		}
	}
}

func Test_decode(t *testing.T) {
	for i := 0; i < 1000; i++ {
		rc := randomNewRemotingCommand()

		bs, err := encode(rc)
		if err != nil {
			t.Fatalf("encode RemotingCommand to bytes fail: %v", err)
		}

		decodedRc, err := decode(bs)
		if err != nil {
			t.Fatalf("decode bytes to RemotingCommand fail: %v", err)
		}

		if !reflect.DeepEqual(*rc, *decodedRc) {
			t.Fatal("decoded RemotingCommand not equal to the original one")
		}
	}
}

func Benchmark_decode(b *testing.B) {
	rc := randomNewRemotingCommand()
	bs, err := encode(rc)
	if err != nil {
		b.Fatalf("encode RemotingCommand to bytes fail: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := decode(bs[4:]); err != nil {
			b.Fatalf("decode bytes to RemotingCommand fail: %v", err)
		}
	}
}

func Test_jsonCodec_encodeHeader(t *testing.T) {
	for i := 0; i < 1000; i++ {
		rc := randomNewRemotingCommand()

		if _, err := jsonSerializer.encodeHeader(rc); err != nil {
			t.Fatalf("encode header with jsonCodec fail: %v", err)
		}
	}
}

func Benchmark_jsonCodec_encodeHeader(b *testing.B) {
	rc := randomNewRemotingCommand()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := jsonSerializer.encodeHeader(rc); err != nil {
			b.Fatalf("encode header with jsonCodec fail: %v", err)
		}
	}
}

func Test_jsonCodec_decodeHeader(t *testing.T) {
	for i := 0; i < 1; i++ {
		rc := randomNewRemotingCommand()

		headers, err := jsonSerializer.encodeHeader(rc)
		if err != nil {
			t.Fatalf("encode header with jsonCodec fail: %v", err)
		}

		decodedRc, err := jsonSerializer.decodeHeader(headers)
		if err != nil {
			t.Fatalf("decode header with jsonCodec fail: %v", err)
		}

		if rc.Code != decodedRc.Code ||
			rc.Language != decodedRc.Language ||
			rc.Version != decodedRc.Version ||
			rc.Opaque != rc.Opaque ||
			rc.Flag != rc.Flag ||
			rc.Remark != rc.Remark ||
			!reflect.DeepEqual(rc.ExtFields, decodedRc.ExtFields) {
			t.Fatal("decoded RemotingCommand not equal to the original one")
		}
	}
}

func Benchmark_jsonCodec_decodeHeader(b *testing.B) {
	rc := randomNewRemotingCommand()
	headers, err := jsonSerializer.encodeHeader(rc)
	if err != nil {
		b.Fatalf("encode header with jsonCodec fail: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := jsonSerializer.decodeHeader(headers); err != nil {
			b.Fatalf("decode header with jsonCodec fail: %v", err)
		}
	}
}

func Test_rmqCodec_encodeHeader(t *testing.T) {
	for i := 0; i < 1000; i++ {
		rc := randomNewRemotingCommand()

		if _, err := rocketMqSerializer.encodeHeader(rc); err != nil {
			t.Fatalf("encode header with rmqCodec fail: %v", err)
		}
	}
}

func Benchmark_rmqCodec_encodeHeader(b *testing.B) {
	rc := randomNewRemotingCommand()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := rocketMqSerializer.encodeHeader(rc); err != nil {
			b.Fatalf("encode header with rmqCodec fail: %v", err)
		}
	}
}

func Test_rmqCodec_decodeHeader(t *testing.T) {
	for i := 0; i < 1; i++ {
		rc := randomNewRemotingCommand()

		headers, err := rocketMqSerializer.encodeHeader(rc)
		if err != nil {
			t.Fatalf("encode header with rmqCodec fail: %v", err)
		}

		decodedRc, err := rocketMqSerializer.decodeHeader(headers)
		if err != nil {
			t.Fatalf("decode header with rmqCodec fail: %v", err)
		}

		if rc.Code != decodedRc.Code ||
			rc.Language != decodedRc.Language ||
			rc.Version != decodedRc.Version ||
			rc.Opaque != rc.Opaque ||
			rc.Flag != rc.Flag ||
			rc.Remark != rc.Remark ||
			!reflect.DeepEqual(rc.ExtFields, decodedRc.ExtFields) {
			t.Fatal("decoded RemotingCommand not equal to the original one")
		}
	}
}

func Benchmark_rmqCodec_decodeHeader(b *testing.B) {
	rc := randomNewRemotingCommand()
	headers, err := rocketMqSerializer.encodeHeader(rc)
	if err != nil {
		b.Fatalf("encode header with rmqCodec fail: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := rocketMqSerializer.decodeHeader(headers); err != nil {
			b.Fatalf("decode header with rmqCodec fail: %v", err)
		}
	}
}

func TestCommandJsonEncodeDecode(t *testing.T) {
	cmd := NewRemotingCommand(192, map[string]string{"brokers": "127.0.0.1"}, []byte("Hello RocketMQCodecs"))
	codecType = JsonCodecs
	cmdData, err := encode(cmd)
	if err != nil {
		t.Errorf("failed to encode remotingCommand in JSON, %s", err)
	} else {
		if len(cmdData) == 0 {
			t.Errorf("failed to encode remotingCommand, result is empty.")
		}
	}
	newCmd, err := decode(cmdData)
	if err != nil {
		t.Errorf("failed to decode remoting in JSON. %s", err)
	}
	if newCmd.Code != cmd.Code {
		t.Errorf("wrong command code. want=%d, got=%d", cmd.Code, newCmd.Code)
	}
	if newCmd.Language != cmd.Language {
		t.Errorf("wrong command language. want=%d, got=%d", cmd.Language, newCmd.Language)
	}
	if newCmd.Version != cmd.Version {
		t.Errorf("wrong command version. want=%d, got=%d", cmd.Version, newCmd.Version)
	}
	if newCmd.Opaque != cmd.Opaque {
		t.Errorf("wrong command version. want=%d, got=%d", cmd.Opaque, newCmd.Opaque)
	}
	if newCmd.Flag != cmd.Flag {
		t.Errorf("wrong commad flag. want=%d, got=%d", cmd.Flag, newCmd.Flag)
	}
	if newCmd.Remark != cmd.Remark {
		t.Errorf("wrong command remakr. want=%s, got=%s", cmd.Remark, newCmd.Remark)
	}
	for k, v := range cmd.ExtFields {
		if vv, ok := newCmd.ExtFields[k]; !ok {
			t.Errorf("key: %s not exists in newCommand.", k)
		} else {
			if v != vv {
				t.Errorf("wrong value. want=%s, got=%s", v, vv)
			}
		}
	}
}

func TestCommandRocketMQEncodeDecode(t *testing.T) {
	cmd := NewRemotingCommand(192, map[string]string{"brokers": "127.0.0.1"}, []byte("Hello RocketMQCodecs"))
	codecType = RocketMQCodecs
	cmdData, err := encode(cmd)
	if err != nil {
		t.Errorf("failed to encode remotingCommand in JSON, %s", err)
	} else {
		if len(cmdData) == 0 {
			t.Errorf("failed to encode remotingCommand, result is empty.")
		}
	}
	newCmd, err := decode(cmdData)
	if err != nil {
		t.Errorf("failed to decode remoting in JSON. %s", err)
	}
	if newCmd.Code != cmd.Code {
		t.Errorf("wrong command code. want=%d, got=%d", cmd.Code, newCmd.Code)
	}
	if newCmd.Language != cmd.Language {
		t.Errorf("wrong command language. want=%d, got=%d", cmd.Language, newCmd.Language)
	}
	if newCmd.Version != cmd.Version {
		t.Errorf("wrong command version. want=%d, got=%d", cmd.Version, newCmd.Version)
	}
	if newCmd.Opaque != cmd.Opaque {
		t.Errorf("wrong command version. want=%d, got=%d", cmd.Opaque, newCmd.Opaque)
	}
	if newCmd.Flag != cmd.Flag {
		t.Errorf("wrong commad flag. want=%d, got=%d", cmd.Flag, newCmd.Flag)
	}
	if newCmd.Remark != cmd.Remark {
		t.Errorf("wrong command remakr. want=%s, got=%s", cmd.Remark, newCmd.Remark)
	}
	for k, v := range cmd.ExtFields {
		if vv, ok := newCmd.ExtFields[k]; !ok {
			t.Errorf("key: %s not exists in newCommand.", k)
		} else {
			if v != vv {
				t.Errorf("wrong value. want=%s, got=%s", v, vv)
			}
		}
	}
}
