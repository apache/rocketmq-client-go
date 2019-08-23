/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package internal

import (
	"errors"
	"regexp"
	"strings"
	"sync"

	"github.com/apache/rocketmq-client-go/internal/remote"
	"github.com/apache/rocketmq-client-go/primitive"
)

var (
	ipRegex, _ = regexp.Compile(`^((25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))\.){3}(25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))`)

	ErrNoNameserver = errors.New("nameServerAddrs can't be empty.")
	ErrMultiIP      = errors.New("multiple IP addr does not support")
	ErrIllegalIP    = errors.New("IP addr error")
)

//go:generate mockgen -source namesrv.go -destination mock_namesrv.go -self_package github.com/apache/rocketmq-client-go/internal  --package internal Namesrvs
type Namesrvs interface {
	AddBroker(routeData *TopicRouteData)

	cleanOfflineBroker()

	UpdateTopicRouteInfo(topic string) *TopicRouteData

	FetchPublishMessageQueues(topic string) ([]*primitive.MessageQueue, error)

	FindBrokerAddrByTopic(topic string) string

	FindBrokerAddrByName(brokerName string) string

	FindBrokerAddressInSubscribe(brokerName string, brokerId int64, onlyThisBroker bool) *FindBrokerResult

	FetchSubscribeMessageQueues(topic string) ([]*primitive.MessageQueue, error)
}

// namesrvs rocketmq namesrv instance.
type namesrvs struct {
	// namesrv addr list
	srvs []string

	// lock for getNameServerAddress in case of update index race condition
	lock sync.Locker

	// index indicate the next position for getNameServerAddress
	index int

	// brokerName -> *BrokerData
	brokerAddressesMap sync.Map

	// brokerName -> map[string]int32
	brokerVersionMap sync.Map

	//subscribeInfoMap sync.Map
	routeDataMap sync.Map

	lockNamesrv sync.Mutex

	nameSrvClient remote.RemotingClient
}

var _ Namesrvs = &namesrvs{}

// NewNamesrv init Namesrv from namesrv addr string.
func NewNamesrv(addr primitive.NamesrvAddr) (*namesrvs, error) {
	if err := addr.Check(); err != nil {
		return nil, err
	}
	nameSrvClient := remote.NewRemotingClient()
	return &namesrvs{
		srvs:          addr,
		lock:          new(sync.Mutex),
		nameSrvClient: nameSrvClient,
	}, nil
}

// getNameServerAddress return namesrv using round-robin strategy.
func (s *namesrvs) getNameServerAddress() string {
	s.lock.Lock()
	defer s.lock.Unlock()

	addr := s.srvs[s.index]
	index := s.index + 1
	if index < 0 {
		index = -index
	}
	index %= len(s.srvs)
	s.index = index
	return strings.TrimLeft(addr, "http(s)://")
}

func (s *namesrvs) Size() int {
	return len(s.srvs)
}

func (s *namesrvs) String() string {
	return strings.Join(s.srvs, ";")
}
func (s *namesrvs) SetCredentials(credentials primitive.Credentials) {
	s.nameSrvClient.RegisterInterceptor(remote.ACLInterceptor(credentials))
}
