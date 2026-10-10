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
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

const (
	DEFAULT_NAMESRV_ADDR = "http://jmenv.tbsite.net:8080/rocketmq/nsaddr"
)

var (
	ipRegex, _ = regexp.Compile(`^((25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))\.){3}(25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))`)

	ErrNoNameserver = errors.New("nameServerAddrs can't be empty.")
	ErrMultiIP      = errors.New("multiple IP addr does not support")
	ErrIllegalIP    = errors.New("IP addr error")
)

//go:generate mockgen -source namesrv.go -destination mock_namesrv.go -self_package github.com/apache/rocketmq-client-go/v2/internal  --package internal Namesrvs
type Namesrvs interface {
	UpdateNameServerAddress()

	AddBroker(routeData *TopicRouteData)

	cleanOfflineBroker()

	UpdateTopicRouteInfo(topic string) (routeData *TopicRouteData, changed bool, err error)

	UpdateTopicRouteInfoWithDefault(topic string, defaultTopic string, defaultQueueNum int) (*TopicRouteData, bool, error)

	FetchPublishMessageQueues(topic string) ([]*primitive.MessageQueue, error)

	FindBrokerAddrByTopic(topic string) string

	FindBrokerAddrByName(brokerName string) string

	FindBrokerAddressInSubscribe(brokerName string, brokerId int64, onlyThisBroker bool) *FindBrokerResult

	FetchSubscribeMessageQueues(topic string) ([]*primitive.MessageQueue, error)

	FetchClusterList(topic string) ([]string, error)

	AddrList() []string
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

	bundleClient *rmqClient

	// brokerName -> map[string]int32: brokerAddr -> version
	brokerVersionMap map[string]map[string]int32
	// lock for broker version read/write
	brokerLock *sync.RWMutex

	//subscribeInfoMap sync.Map
	routeDataMap sync.Map

	lockNamesrv sync.Mutex

	nameSrvClient  remote.RemotingClient
	remotingConfig *remote.RemotingClientConfig
	credentials    *primitive.Credentials // protected by lock

	resolver primitive.NsResolver
}

var _ Namesrvs = (*namesrvs)(nil)

func GetNamesrv(clientId string) (*namesrvs, error) {
	actual, ok := clientMap.Load(clientId)
	if !ok {
		return nil, fmt.Errorf("the namesrv in instanceName [%s] not found", clientId)
	}
	return actual.(*rmqClient).GetNameSrv().(*namesrvs), nil
}

// NewNamesrv init Namesrv from namesrv addr string.
// addr primitive.NamesrvAddr
func NewNamesrv(resolver primitive.NsResolver, config *remote.RemotingClientConfig) (*namesrvs, error) {
	addr := resolver.Resolve()
	if len(addr) == 0 {
		return nil, errors.New("no name server addr found with resolver: " + resolver.Description())
	}

	if err := primitive.NamesrvAddr(addr).Check(); err != nil {
		return nil, err
	}
	return newNamesrvWithAddresses(addr, resolver, config), nil
}

func newNamesrvWithAddresses(addr []string, resolver primitive.NsResolver, config *remote.RemotingClientConfig) *namesrvs {
	return &namesrvs{
		srvs:             append([]string(nil), addr...),
		lock:             new(sync.Mutex),
		nameSrvClient:    remote.NewRemotingClient(config),
		remotingConfig:   config,
		brokerVersionMap: make(map[string]map[string]int32, 0),
		brokerLock:       new(sync.RWMutex),
		resolver:         resolver,
	}
}

// cloneForClient retains discovery configuration but gives a new client its own
// transport and route caches. Do not resolve again: discovery can temporarily be
// unavailable, while the last known address snapshot is still usable.
func (s *namesrvs) cloneForClient() *namesrvs {
	s.lock.Lock()
	defer s.lock.Unlock()
	cloned := newNamesrvWithAddresses(s.srvs, s.resolver, s.remotingConfig)
	if s.credentials != nil {
		cloned.SetCredentials(*s.credentials)
	}
	return cloned
}

// getNameServerAddress return namesrv using round-robin strategy.
func (s *namesrvs) getNameServerAddress() string {
	s.lock.Lock()
	defer s.lock.Unlock()

	addr := s.srvs[s.index%len(s.srvs)]
	index := s.index + 1
	if index < 0 {
		index = -index
	}
	index %= len(s.srvs)
	s.index = index
	if strings.HasPrefix(addr, "https") {
		return strings.TrimPrefix(addr, "https://")
	}
	return strings.TrimPrefix(addr, "http://")
}

func (s *namesrvs) Size() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return len(s.srvs)
}

func (s *namesrvs) String() string {
	return strings.Join(s.AddrList(), ";")
}
func (s *namesrvs) SetCredentials(credentials primitive.Credentials) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.credentials = &credentials
	s.nameSrvClient.RegisterInterceptor(remote.ACLInterceptor(credentials))
}

func (s *namesrvs) AddrList() []string {
	s.lock.Lock()
	defer s.lock.Unlock()
	return append([]string(nil), s.srvs...)
}

// UpdateNameServerAddress will update srvs.
// docs: https://rocketmq.apache.org/docs/best-practice-namesvr/
func (s *namesrvs) UpdateNameServerAddress() {
	s.lock.Lock()
	defer s.lock.Unlock()

	srvs := s.resolver.Resolve()
	if len(srvs) == 0 {
		return
	}

	updated := primitive.Diff(s.srvs, srvs)
	if !updated {
		return
	}

	s.srvs = append([]string(nil), srvs...)
}
