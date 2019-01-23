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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync/atomic"
)

var opaque int32

const (
	// 0, REQUEST_COMMAND
	RpcType = 0

	// 1, RPC
	RpcOneWay = 1

	ResponseType = 1

	RemotingCommandFlag    = 0
	languageFlag           = "golang"
	remotingCommandVersion = 137
)

type remotingCommand struct {
	Code      int16             `json:"code"`
	Language  string            `json:"language"` //int 8
	Version   int16             `json:"version"`
	Opaque    int32             `json:"opaque"`
	Flag      int               `json:"flag"`
	Remark    string            `json:"remark"`
	ExtFields map[string]string `json:"extFields"`
	Body      []byte            `json:"body,omitempty"`
}

func newRemotingCommand(code int16, properties map[string]string, body []byte) *remotingCommand {
	remotingCommand := &remotingCommand{
		Code:      code,
		Language:  languageFlag,
		Version:   remotingCommandVersion,
		Opaque:    atomic.AddInt32(&opaque, 1),
		ExtFields: properties,
		Body:      body,
	}

	return remotingCommand
}

func (rcm *remotingCommand) isResponseType() bool {
	return rcm.Flag&(ResponseType) == ResponseType
}

func (rcm *remotingCommand) markResponseType() {
	rcm.Flag = rcm.Flag | ResponseType
}

type CodecType int

const (
	Json CodecType = iota
	RocketMQ
)

var codec serializer

type serializer interface{
	encode(command *remotingCommand) ([]byte, error)
	decode(headerArray, body []byte) (*remotingCommand, error)
}

type jsonCodeC int

func (c *jsonCodeC) encode(command *remotingCommand) ([]byte, error) {
	buf, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (c *jsonCodeC) decode(header, body []byte) (*remotingCommand, error) {
	cmd := &remotingCommand{}
	cmd.ExtFields = make(map[string]string)
	err := json.Unmarshal(header, cmd)
	if err != nil {
		return nil, err
	}
	cmd.Body = body
	return cmd, nil
}

type rmqCodec int

func (c *rmqCodec) encode(cmd *remotingCommand) ([]byte, error) {
	var (
		err error
		remarkBytes       []byte
		remarkBytesLen    int
		extFieldsBytes    []byte
		extFieldsBytesLen int
	)
	remarkBytesLen = 0
	if len(cmd.Remark) > 0 {
		remarkBytes = []byte(cmd.Remark)
		remarkBytesLen = len(remarkBytes)
	}
	if cmd.ExtFields != nil {
		buf := bytes.NewBuffer([]byte{})
		for key, value := range cmd.ExtFields {
			keyBytes := []byte(fmt.Sprintf("%v", key))
			valueBytes := []byte(fmt.Sprintf("%v", value))
			err = binary.Write(buf, binary.BigEndian, int16(len(keyBytes)))
			if err != nil {
				return nil, err
			}
			buf.Write(keyBytes)

			err = binary.Write(buf, binary.BigEndian, int32(len(valueBytes)))
			if err != nil {
				return nil, err
			}
			buf.Write(valueBytes)
		}

		extFieldsBytes = buf.Bytes()
		extFieldsBytesLen = len(extFieldsBytes)
	}
	buf := bytes.NewBuffer([]byte{})

	// code(~32767) 2
	err = binary.Write(buf, binary.BigEndian, int16(cmd.Code))
	if err != nil {
		return nil, err
	}

	// Golang
	err = binary.Write(buf, binary.BigEndian, int8(0))
	if err != nil {
		return nil, err
	}

	// 2
	err = binary.Write(buf, binary.BigEndian, int16(cmd.Version))
	if err != nil {
		return nil, err
	}

	// opaque 4
	err = binary.Write(buf, binary.BigEndian, int32(cmd.Opaque))
	if err != nil {
		return nil, err
	}

	// 4
	err = binary.Write(buf, binary.BigEndian, int32(cmd.Flag))
	if err != nil {
		return nil, err
	}

	// 4
	err = binary.Write(buf, binary.BigEndian, int32(remarkBytesLen))
	if err != nil {
		return nil, err
	}

	if remarkBytesLen > 0 {
		buf.Write(remarkBytes)
	}

	// 4
	err = binary.Write(buf, binary.BigEndian, int32(extFieldsBytesLen))
	if err != nil {
		return nil, err
	}

	if extFieldsBytesLen > 0 {
		buf.Write(extFieldsBytes)
	}

	return buf.Bytes(), nil
}

func (c *rmqCodec) decode(headerArray, body []byte) (*remotingCommand, error) {
	var err error
	cmd := &remotingCommand{}
	buf := bytes.NewBuffer(headerArray)
	// int code(~32767)
	err = binary.Read(buf, binary.BigEndian, &cmd.Code)
	if err != nil {
		return nil, err
	}

	// LanguageCode language
	var LanguageCodeNope byte
	err = binary.Read(buf, binary.BigEndian, &LanguageCodeNope)
	cmd.Language = languageFlag
	if err != nil {
		return nil, err
	}

	// int version(~32767)
	err = binary.Read(buf, binary.BigEndian, &cmd.Version)
	if err != nil {
		return nil, err
	}

	// int opaque
	err = binary.Read(buf, binary.BigEndian, &cmd.Opaque)
	if err != nil {
		return nil, err
	}

	// int flag
	err = binary.Read(buf, binary.BigEndian, &cmd.Flag)
	if err != nil {
		return nil, err
	}

	// String remark
	var remarkLen, extFieldsLen int32
	err = binary.Read(buf, binary.BigEndian, &remarkLen)
	if err != nil {
		return nil, err
	}

	if remarkLen > 0 {
		var remarkData = make([]byte, remarkLen)
		err = binary.Read(buf, binary.BigEndian, &remarkData)
		if err != nil {
			return nil, err
		}
		cmd.Remark = string(remarkData)
	}

	// map ext
	// HashMap<String, String> extFields
	err = binary.Read(buf, binary.BigEndian, &extFieldsLen)
	if err != nil {
		return nil, err
	}

	if extFieldsLen > 0 {
		var extFieldsData = make([]byte, extFieldsLen)
		err = binary.Read(buf, binary.BigEndian, &extFieldsData)
		if err != nil {
			return nil, err
		}

		cmd.ExtFields = make(map[string]string)
		buf := bytes.NewBuffer(extFieldsData)
		for buf.Len() > 0 {
			var kLen int16
			err = binary.Read(buf, binary.BigEndian, &kLen)
			if err != nil {
				return nil, err
			}

			key, err := getExtFieldsData(buf, int32(kLen))
			if err != nil {
				return nil, err
			}

			var vLen int32
			err = binary.Read(buf, binary.BigEndian, &vLen)
			if err != nil {
				return nil, err
			}

			value, err := getExtFieldsData(buf, vLen)
			if err != nil {
				return nil, err
			}
			cmd.ExtFields[key] = value
		}
	}

	cmd.Body = body
	return cmd, nil
}

func getExtFieldsData(buff *bytes.Buffer, length int32) (string, error) {
	var data = make([]byte, length)
	err := binary.Read(buff, binary.BigEndian, &data)
	if err != nil {
		return "", err
	}

	return string(data), nil
}