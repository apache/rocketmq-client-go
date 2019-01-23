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
	"github.com/apache/rocketmq-externals/rocketmq-go/util"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"strconv"
	"strings"
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
	InvokeSync(addr string, request *remotingCommand, timeoutMillis int64) (remotingCommand *remotingCommand, err error)
	InvokeAsync(addr string, request *remotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error
	InvokeOneWay(addr string, request *remotingCommand, timeoutMillis int64) error
}
type defaultClient struct {
	clientId     string
	clientConfig *ClientConfig

	connTable     map[string]net.Conn
	connTableLock sync.RWMutex

	responseTable  util.ConcurrentMap //map[int32]*ResponseFuture
	processorTable util.ConcurrentMap //map[int]ClientRequestProcessor //requestCode|ClientRequestProcessor
	//	protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
	//new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(64);
	namesrvAddrList          []string
	namesrvAddrSelectedAddr  string
	namesrvAddrSelectedIndex int                    //how to chose. done
	namesvrLockRW            sync.RWMutex           //
	clientRequestProcessor   ClientRequestProcessor //mange register the processor here
	codec        serializer      //rocketmq encode decode
}

func RemotingClientInit(clientConfig *ClientConfig, clientRequestProcessor ClientRequestProcessor) (client *defaultClient) {
	client = &defaultClient{}
	client.connTable = map[string]net.Conn{}
	client.responseTable = util.New()
	client.clientConfig = clientConfig

	client.namesrvAddrList = strings.Split(clientConfig.nameServerAddress, ";")
	client.namesrvAddrSelectedIndex = -1
	client.clientRequestProcessor = clientRequestProcessor
	//client.serializerHandler = NewSerializerHandler()
	return
}

func (client *defaultClient) InvokeSync(addr string, request *remotingCommand, timeoutMillis int64) (remotingCommand *remotingCommand, err error) {
	var conn net.Conn
	conn, err = client.GetOrCreateConn(addr)
	response := &ResponseFuture{
		SendRequestOK:  false,
		Opaque:         request.Opaque,
		TimeoutMillis:  timeoutMillis,
		BeginTimestamp: time.Now().Unix(),
		Done:           make(chan bool),
	}
	header, err := client.codec.encode(request)
	body := request.Body
	client.SetResponse(request.Opaque, response)
	err = client.sendRequest(header, body, conn, addr)
	if err != nil {
		log.Error(err)
		return
	}
	select {
	case <-response.Done:
		remotingCommand = response.ResponseCommand
		return
	case <-time.After(time.Duration(timeoutMillis) * time.Millisecond):
		err = errors.New("invoke sync timeout:" + strconv.FormatInt(timeoutMillis, 10) + " Millisecond")
		return
	}
}

func (client *defaultClient) InvokeAsync(addr string, request *remotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error {
	conn, err := client.GetOrCreateConn(addr)
	if err != nil {
		return err
	}
	response := &ResponseFuture{
		SendRequestOK:  false,
		Opaque:         request.Opaque,
		TimeoutMillis:  timeoutMillis,
		BeginTimestamp: time.Now().Unix(),
		InvokeCallback: invokeCallback,
	}
	client.SetResponse(request.Opaque, response)
	header, err := client.codec.encode(request)
	body := request.Body
	err = client.sendRequest(header, body, conn, addr)
	if err != nil {
		log.Error(err)
		return err
	}
	return err
}
func (client *defaultClient) InvokeOneWay(addr string, request *remotingCommand, timeoutMillis int64) error {
	conn, err := client.GetOrCreateConn(addr)
	if err != nil {
		return err
	}
	header, err := client.codec.encode(request)
	body := request.Body
	err = client.sendRequest(header, body, conn, addr)
	if err != nil {
		log.Error(err)
		return err
	}
	return err
}

func (client *defaultClient) sendRequest(header, body []byte, conn net.Conn, addr string) error {
	var requestBytes []byte
	requestBytes = append(requestBytes, header...)
	if body != nil && len(body) > 0 {
		requestBytes = append(requestBytes, body...)
	}
	_, err := conn.Write(requestBytes)
	if err != nil {
		log.Error(err)
		if len(addr) > 0 {
			client.ReleaseConn(addr, conn)
		}
		return err
	}
	return nil
}

func (client *defaultClient) GetNamesrvAddrList() []string {
	return client.namesrvAddrList
}

func (client *defaultClient) SetResponse(index int32, response *ResponseFuture) {
	client.responseTable.Set(strconv.Itoa(int(index)), response)
}

func (client *defaultClient) getResponse(index int32) (response *ResponseFuture, err error) {
	obj, ok := client.responseTable.Get(strconv.Itoa(int(index)))
	if !ok {
		err = errors.New("get conn from responseTable error")
		return
	}
	response = obj.(*ResponseFuture)
	return
}

func (client *defaultClient) removeResponse(index int32) {
	client.responseTable.Remove(strconv.Itoa(int(index)))
}

func (client *defaultClient) GetOrCreateConn(address string) (conn net.Conn, err error) {
	if len(address) == 0 {
		conn, err = client.getNamesvrConn()
		return
	}
	conn = client.GetConn(address)
	if conn != nil {
		return
	}
	conn, err = client.CreateConn(address)
	return
}

func (client *defaultClient) GetConn(address string) (conn net.Conn) {
	client.connTableLock.RLock()
	conn = client.connTable[address]
	client.connTableLock.RUnlock()
	return
}

func (client *defaultClient) CreateConn(address string) (conn net.Conn, err error) {
	defer client.connTableLock.Unlock()
	client.connTableLock.Lock()
	conn = client.connTable[address]
	if conn != nil {
		return
	}
	conn, err = client.createAndHandleTcpConn(address)
	client.connTable[address] = conn
	return
}

func (client *defaultClient) getNamesvrConn() (conn net.Conn, err error) {
	client.namesvrLockRW.RLock()
	address := client.namesrvAddrSelectedAddr
	client.namesvrLockRW.RUnlock()
	if len(address) != 0 {
		conn = client.GetConn(address)
		if conn != nil {
			return
		}
	}

	defer client.namesvrLockRW.Unlock()
	client.namesvrLockRW.Lock()
	//already connected by another write lock owner
	address = client.namesrvAddrSelectedAddr
	if len(address) != 0 {
		conn = client.GetConn(address)
		if conn != nil {
			return
		}
	}

	addressCount := len(client.namesrvAddrList)
	if client.namesrvAddrSelectedIndex < 0 {
		client.namesrvAddrSelectedIndex = rand.Intn(addressCount)
	}
	for i := 1; i <= addressCount; i++ {
		selectedIndex := (client.namesrvAddrSelectedIndex + i) % addressCount
		selectAddress := client.namesrvAddrList[selectedIndex]
		if len(selectAddress) == 0 {
			continue
		}
		conn, err = client.CreateConn(selectAddress)
		if err == nil {
			client.namesrvAddrSelectedAddr = selectAddress
			client.namesrvAddrSelectedIndex = selectedIndex
			return
		}
	}
	err = errors.New("all namesvrAddress can't use!,address:" + client.clientConfig.nameServerAddress)
	return
}

func (client *defaultClient) createAndHandleTcpConn(address string) (conn net.Conn, err error) {
	conn, err = net.Dial("tcp", address)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	go client.handlerReceiveLoop(conn, address) //handler连接 处理这个连接返回的结果
	return
}

func (client *defaultClient) ReleaseConn(addr string, conn net.Conn) {
	defer client.connTableLock.Unlock()
	conn.Close()
	client.connTableLock.Lock()
	delete(client.connTable, addr)
}

func (client *defaultClient) handlerReceiveLoop(conn net.Conn, addr string) (err error) {
	defer func() {
		//when for is break releaseConn
		log.Error(err, addr)
		client.ReleaseConn(addr, conn)
	}()
	b := make([]byte, 1024)
	var length, headerLength, bodyLength int32
	var buf = bytes.NewBuffer([]byte{})
	var header, body []byte
	var readTotalLengthFlag = true //readLen when true,read data when false
	for {
		var n int
		n, err = conn.Read(b)
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
			go client.handlerReceivedMessage(conn, headerSerializableType, header, body)
		}
	}
}

func (client *defaultClient) handlerReceivedMessage(conn net.Conn, headerSerializableType byte, headBytes []byte, bodyBytes []byte) {
	cmd, _ := client.codec.decode(headBytes, bodyBytes)
	if cmd.isResponseType() {
		client.handlerResponse(cmd)
		return
	}
	go client.handlerRequest(conn, cmd)
}

func (client *defaultClient) handlerRequest(conn net.Conn, cmd *remotingCommand) {
	responseCommand := client.clientRequestProcessor(cmd)
	if responseCommand == nil {
		return
	}
	responseCommand.Opaque = cmd.Opaque
	responseCommand.markResponseType()
	header, err := client.codec.encode(responseCommand)
	body := responseCommand.Body
	err = client.sendRequest(header, body, conn, "")
	if err != nil {
		log.Error(err)
	}
}

func (client *defaultClient) handlerResponse(cmd *remotingCommand) {
	response, err := client.getResponse(cmd.Opaque)
	client.removeResponse(cmd.Opaque)
	if err != nil {
		return
	}
	response.ResponseCommand = cmd
	if response.InvokeCallback != nil {
		response.InvokeCallback(response)
	}

	if response.Done != nil {
		response.Done <- true
	}
}

func (client *defaultClient) ClearExpireResponse() {
	for seq, responseObj := range client.responseTable.Items() {
		response := responseObj.(*ResponseFuture)
		if (response.BeginTimestamp + 30) <= time.Now().Unix() {
			//30 minutes expired
			client.responseTable.Remove(seq)
			if response.InvokeCallback != nil {
				response.InvokeCallback(nil)
				log.Warningf("remove time out request %v", response)
			}
		}
	}
}
