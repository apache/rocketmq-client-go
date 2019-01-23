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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
	"time"
)

type CommunicationMode int

const (
	Sync CommunicationMode = iota
	Async
	OneWay
)

type RemotingClient interface {
	InvokeSync(context context.Context, request *remotingCommand) (remotingCommand *remotingCommand, err error)
	InvokeAsync(context context.Context, request *remotingCommand, f func(*remotingCommand)) error
	InvokeOneWay(context context.Context, request *remotingCommand) error
}

type defaultClient struct {
	//clientId     string
	remoteAddress string
	conn net.Conn
	// requestId
	opaque int32

	// int32 -> ResponseFuture
	responseTable sync.Map
	codec         serializer
	exitCh chan interface{}
}

func NewRemotingClient(remoteAddress string, cType CodecType) (*defaultClient, error) {

	client := &defaultClient{
		remoteAddress: remoteAddress,
	}

	switch cType {
	case Json:
		client.codec = &jsonCodec{}
	case RocketMQ:
		client.codec = &rmqCodec{}
	default:
		return nil, errors.New("unknow codec")
	}

	conn, err := net.Dial("tcp", remoteAddress)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	client.conn = conn
	go client.listen()
	go client.clearExpiredRequest()
	return client, nil
}

func (client *defaultClient) InvokeSync(request *remotingCommand, timeout time.Duration) (*remotingCommand, error) {

	response := &ResponseFuture{
		SendRequestOK:  false,
		Opaque:         request.Opaque,
		TimeoutMillis:  timeout,
		BeginTimestamp: time.Now().Unix(),
		Done:           make(chan bool),
	}
	header, err := client.codec.encode(request)
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
	case <-time.After(timeout * time.Millisecond):
		return nil, fmt.Errorf("invoke sync timeout")
	}
}

func (client *defaultClient) InvokeAsync(request *remotingCommand, timeout time.Duration,
	callback func(*remotingCommand)) error {

	response := &ResponseFuture{
		SendRequestOK:  false,
		Opaque:         request.Opaque,
		TimeoutMillis:  timeout,
		BeginTimestamp: time.Now().Unix(),
		callback:       callback,
	}
	client.responseTable.Store(request.Opaque, response)
	header, err := client.codec.encode(request)
	body := request.Body
	err = client.doRequest(header, body)
	if err != nil {
		log.Error(err)
		return err
	}
	return err
}

func (client *defaultClient) InvokeOneWay(request *remotingCommand, timeout time.Duration) error {
	header, err := client.codec.encode(request)
	body := request.Body
	err = client.doRequest(header, body)
	if err != nil {
		log.Error(err)
		return err
	}
	return err
}

func (client *defaultClient) doRequest(header, body []byte) error {
	var requestBytes []byte
	requestBytes = append(requestBytes, header...)
	if body != nil && len(body) > 0 {
		requestBytes = append(requestBytes, body...)
	}
	_, err := client.conn.Write(requestBytes)
	if err != nil {
		log.Error(err)
		if len(client.remoteAddress) > 0 {
			client.close()
		}
		return err
	}
	return nil
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
	var readTotalLengthFlag = true //readLen when true,read data when false
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
	cmd, _ := client.codec.decode(headBytes, bodyBytes)
	if cmd.isResponseType() {
		client.handlerResponse(cmd)
		return
	}
	go client.handlerRequestFromServer(cmd)
}

func (client *defaultClient) handlerRequestFromServer(cmd *remotingCommand) {
	//responseCommand := client.clientRequestProcessor(cmd)
	//if responseCommand == nil {
	//	return
	//}
	//responseCommand.Opaque = cmd.Opaque
	//responseCommand.markResponseType()
	//header, err := client.codec.encode(responseCommand)
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
