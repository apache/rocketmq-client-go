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
	"errors"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
	"time"
)

var (
	ErrRequestTimeout = errors.New("request timeout")
)

type RemotingClient interface {
	InvokeSync(request *remotingCommand) (*remotingCommand, error)
	InvokeAsync(request *remotingCommand, f func(*remotingCommand)) error
	InvokeOneWay(request *remotingCommand) error
}

// ClientConfig common config
type ClientConfig struct {
	// NameServer or Broker address
	RemotingAddress string

	ClientIP     string
	InstanceName string

	// Heartbeat interval in microseconds with message broker, default is 30
	HeartbeatBrokerInterval time.Duration

	// request timeout time
	RequestTimeout time.Duration
	CType byte

	UnitMode          bool
	UnitName          string
	VipChannelEnabled bool
}

type defaultClient struct {
	//clientId     string
	config ClientConfig
	conn net.Conn
	// requestId
	opaque int32

	// int32 -> ResponseFuture
	responseTable sync.Map
	codec         serializer
	exitCh chan interface{}
}

func NewRemotingClient(config ClientConfig) (RemotingClient, error) {
	client := &defaultClient{
		config: config,
	}

	switch config.CType {
	case Json:
		client.codec = &jsonCodec{}
	case RocketMQ:
		client.codec = &rmqCodec{}
	default:
		return nil, errors.New("unknow codec")
	}

	conn, err := net.Dial("tcp", config.RemotingAddress)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	client.conn = conn
	go client.listen()
	go client.clearExpiredRequest()
	return client, nil
}

func (client *defaultClient) InvokeSync(request *remotingCommand) (*remotingCommand, error) {

	response := &ResponseFuture{
		SendRequestOK:  false,
		Opaque:         request.Opaque,
		TimeoutMillis:  client.config.RequestTimeout,
		BeginTimestamp: time.Now().Unix(),
		Done:           make(chan bool),
	}
	header, err := encode(request)
	body := request.Body
	client.responseTable.Store(request.Opaque, response)
	err = client.doRequest(header, body)

	if err != nil {
		log.Error(err)
		return nil, err
	}
	select {
	case <-response.Done:
		rmd := response.ResponseCommand
		return rmd, nil
	case <-time.After(client.config.RequestTimeout):
		return nil, ErrRequestTimeout
	}
}

func (client *defaultClient) InvokeAsync(request *remotingCommand, f func(*remotingCommand)) error {

	response := &ResponseFuture{
		SendRequestOK:  false,
		Opaque:         request.Opaque,
		TimeoutMillis:  client.config.RequestTimeout,
		BeginTimestamp: time.Now().Unix(),
		callback:       f,
	}
	client.responseTable.Store(request.Opaque, response)
	header, err := encode(request)
	if err != nil {
		return err
	}

	body := request.Body
	return client.doRequest(header, body)
}

func (client *defaultClient) InvokeOneWay(request *remotingCommand) error {
	header, err := encode(request)
	if err != nil {
		return err
	}

	body := request.Body
	return client.doRequest(header, body)
}

func (client *defaultClient) doRequest(header, body []byte) error {
	var requestBytes []byte
	requestBytes = append(requestBytes, header...)
	if body != nil && len(body) > 0 {
		requestBytes = append(requestBytes, body...)
	}
	_, err := client.conn.Write(requestBytes)
	return err
}

func (client *defaultClient) close() {
	// TODO process response
	client.conn.Close()
}

func (client *defaultClient) listen() {
	b := make([]byte, 1024)
	var length, headerLength, bodyLength int32
	var buf = bytes.NewBuffer([]byte{})
	var header, body []byte
	var readTotalLengthFlag = true // readLen when true, read data when false
	for {
		var n int
		n, err := client.conn.Read(b)
		if err != nil {
			return
		}
		_, err = buf.Write(b[:n])
		if err != nil {
			return
		}
		for {
			if readTotalLengthFlag {
				//we read 4 bytes of allDataLength
				if buf.Len() >= 4 {
					err = binary.Read(buf, binary.BigEndian, &length)
					if err != nil {
						return
					}
					readTotalLengthFlag = false //now turn to read data
				} else {
					break //wait bytes we not got
				}
			}
			if !readTotalLengthFlag {
				if buf.Len() < int(length) {
					// judge all data received.if not,loop to wait
					break
				}
			}
			//now all data received, we can read totalLen again
			readTotalLengthFlag = true

			//get the data,and handler it
			//header len
			err = binary.Read(buf, binary.BigEndian, &headerLength)
			var realHeaderLen = headerLength & 0x00ffffff
			//headerData the first ff is about serializable type
			var headerSerializableType = byte(headerLength >> 24)
			header = make([]byte, realHeaderLen)
			_, err = buf.Read(header)
			bodyLength = length - 4 - realHeaderLen
			body = make([]byte, int(bodyLength))
			if bodyLength == 0 {
				// no body
			} else {
				_, err = buf.Read(body)
			}
			go client.handlerReceivedMessage(client.conn, headerSerializableType, header, body)
		}
	}
}

func (client *defaultClient) handlerReceivedMessage(conn net.Conn, headerSerializableType byte, headBytes []byte, bodyBytes []byte) {
	//cmd, _ := decode(headBytes, bodyBytes)
	//if cmd.isResponseType() {
	//	client.handlerResponse(cmd)
	//	return
	//}
	//go client.handlerRequestFromServer(cmd)
}

func (client *defaultClient) handlerRequestFromServer(cmd *remotingCommand) {
	//responseCommand := client.clientRequestProcessor(cmd)
	//if responseCommand == nil {
	//	return
	//}
	//responseCommand.Opaque = cmd.Opaque
	//responseCommand.markResponseType()
	//header, err := encode(responseCommand)
	//body := responseCommand.Body
	//err = client.doRequest(header, body)
	//if err != nil {
	//	log.Error(err)
	//}
}

func (client *defaultClient) handlerResponse(cmd *remotingCommand) error {
	//response, err := client.getResponse(cmd.Opaque)
	////client.removeResponse(cmd.Opaque)
	//if err != nil {
	//	return err
	//}
	//
	//response.ResponseCommand = cmd
	//response.callback(cmd)
	//
	//if response.Done != nil {
	//	response.Done <- true
	//}
	return nil
}

func (client *defaultClient) clearExpiredRequest() {
	//for seq, responseObj := range client.responseTable.Items() {
	//	response := responseObj.(*ResponseFuture)
	//	if (response.BeginTimestamp + 30) <= time.Now().Unix() {
	//		//30 minutes expired
	//		client.responseTable.Remove(seq)
	//		response.callback(nil)
	//		log.Warningf("remove time out request %v", response)
	//	}
	//}
}
