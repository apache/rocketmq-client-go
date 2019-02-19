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
	"testing"
)

func TestCommandJsonEncodeDecode(t *testing.T){
	cmd :=newRemotingCommand(int32(192), map[string]string{"brokers":"127.0.0.1"}, []byte("Hello RocketMQCodecs"))
	codecType = JsonCodecs
	cmdData, err:= encode(cmd)
	if err != nil {
		t.Errorf("failed to encode remotingCommand in JSON, %s", err)
	}else{
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


func TestCommandRocketMQEncodeDecode(t *testing.T){
	cmd :=newRemotingCommand(int32(192), map[string]string{"brokers":"127.0.0.1"}, []byte("Hello RocketMQCodecs"))
	codecType = RocketMQCodecs
	cmdData, err:= encode(cmd)
	if err != nil {
		t.Errorf("failed to encode remotingCommand in JSON, %s", err)
	}else{
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