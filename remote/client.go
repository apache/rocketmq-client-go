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
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/utils"
	log "github.com/sirupsen/logrus"
)

var (
	ErrRequestTimeout = errors.New("request timeout")
)

type RemotingClient interface {
	InvokeSync(request *RemotingCommand) (*RemotingCommand, error)
	InvokeAsync(request *RemotingCommand, f func(*RemotingCommand)) error
	InvokeOneWay(request *RemotingCommand) error
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
	CType          byte

	UnitMode          bool
	UnitName          string
	VipChannelEnabled bool
}

type defaultClient struct {
	//clientId     string
	config ClientConfig
	conn   net.Conn
	// requestId
	opaque int32

	// int32 -> ResponseFuture
	responseTable sync.Map
	codec         serializer
	exitCh        chan interface{}
}

func NewRemotingClient(config ClientConfig) (RemotingClient, error) {
	client := &defaultClient{
		config: config,
	}

	switch config.CType {
	case JSON:
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

func (client *defaultClient) InvokeSync(request *RemotingCommand) (*RemotingCommand, error) {
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

func (client *defaultClient) InvokeAsync(request *RemotingCommand, f func(*RemotingCommand)) error {
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

func (client *defaultClient) InvokeOneWay(request *RemotingCommand) error {
	header, err := encode(request)
	if err != nil {
		return err
	}

	body := request.Body
	return client.doRequest(header, body)
}

func (client *defaultClient) doRequest(header, body []byte) error {
	var requestBytes = make([]byte, len(header)+len(body))
	copy(requestBytes, header)
	if len(body) > 0 {
		copy(requestBytes[len(header):], body)
	}

	_, err := client.conn.Write(requestBytes)
	return err
}

func (client *defaultClient) close() {
	// TODO process response
	client.conn.Close()
}

func (client *defaultClient) listen() {
	rb := utils.NewRingBuffer(4096)

	var frameSize int32
	go func() {
		for {
			err := binary.Read(rb, binary.BigEndian, &frameSize)
			if err != nil {
				// TODO
			}
			data := make([]byte, frameSize)

			_, err = rb.Read(data)

			if err != nil {
				// TODO
			}

			cmd, err := decode(data)
			if cmd.isResponseType() {
				client.handleResponse(cmd)
			} else {
				client.handleRequestFromServer(cmd)
			}
		}
	}()

	buf := make([]byte, 4096)
	for {
		n, err := client.conn.Read(buf)
		if err != nil {
			log.Errorf("read data from connection errors: %v", err)
			return
		}
		err = rb.Write(buf[:n])
		if err != nil {
			// just log
			log.Errorf("write data to buffer errors: %v", err)
		}

	}
}

func (client *defaultClient) handleRequestFromServer(cmd *RemotingCommand) {
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

func (client *defaultClient) handleResponse(cmd *RemotingCommand) error {
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
