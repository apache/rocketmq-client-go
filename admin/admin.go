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

package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/internal/utils"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

type Admin interface {
	CreateTopic(ctx context.Context, opts ...OptionCreate) error
	DeleteTopic(ctx context.Context, opts ...OptionDelete) error

	GetAllSubscriptionGroup(ctx context.Context, brokerAddr string, timeoutMillis time.Duration) (*SubscriptionGroupWrapper, error)
	FetchAllTopicList(ctx context.Context) (*TopicList, error)
	FindBrokerAddrByName(ctx context.Context, BrokerName string) ([]string, error)
	GetBrokerClusterInfo(ctx context.Context) (*ClusterInfo, error)
	FetchPublishMessageQueues(ctx context.Context, topic string) ([]*primitive.MessageQueue, error)
	FetchClusterList(topic string) ([]string, error)
	Close() error
}

// TODO: move outdated context to ctx
type adminOptions struct {
	internal.ClientOptions
}

type AdminOption func(options *adminOptions)

func defaultAdminOptions() *adminOptions {
	opts := &adminOptions{
		ClientOptions: internal.DefaultClientOptions(),
	}
	opts.GroupName = "TOOLS_ADMIN"
	opts.InstanceName = time.Now().String()
	return opts
}

// WithResolver nameserver resolver to fetch nameserver addr
func WithResolver(resolver primitive.NsResolver) AdminOption {
	return func(options *adminOptions) {
		options.Resolver = resolver
	}
}

func WithCredentials(c primitive.Credentials) AdminOption {
	return func(options *adminOptions) {
		options.ClientOptions.Credentials = c
	}
}

// WithNamespace set the namespace of admin
func WithNamespace(namespace string) AdminOption {
	return func(options *adminOptions) {
		options.ClientOptions.Namespace = namespace
	}
}

func WithTls(useTls bool) AdminOption {
	return func(options *adminOptions) {
		options.ClientOptions.RemotingClientConfig.UseTls = useTls
	}
}

type admin struct {
	cli internal.RMQClient

	opts *adminOptions

	closeOnce sync.Once
}

// NewAdmin initialize admin
func NewAdmin(opts ...AdminOption) (*admin, error) {
	defaultOpts := defaultAdminOptions()
	for _, opt := range opts {
		opt(defaultOpts)
	}
	namesrv, err := internal.NewNamesrv(defaultOpts.Resolver, defaultOpts.RemotingClientConfig)
	defaultOpts.Namesrv = namesrv
	if err != nil {
		return nil, err
	}
	if !defaultOpts.Credentials.IsEmpty() {
		namesrv.SetCredentials(defaultOpts.Credentials)
	}

	cli := internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions, nil)
	if cli == nil {
		return nil, fmt.Errorf("GetOrNewRocketMQClient faild")
	}
	defaultOpts.Namesrv = cli.GetNameSrv()
	// log.Printf("Client: %#v", namesrv.srvs)
	return &admin{
		cli:  cli,
		opts: defaultOpts,
	}, nil
}

func (a *admin) GetAllSubscriptionGroup(ctx context.Context, brokerAddr string, timeoutMillis time.Duration) (*SubscriptionGroupWrapper, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetAllSubscriptionGroupConfig, nil, nil)
	a.cli.RegisterACL()
	response, err := a.cli.InvokeSync(ctx, brokerAddr, cmd, timeoutMillis)
	if err != nil {
		rlog.Error("Get all group list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("Get all group list success", map[string]interface{}{})
	}
	var subscriptionGroupWrapper SubscriptionGroupWrapper
	_, err = subscriptionGroupWrapper.Decode(response.Body, &subscriptionGroupWrapper)
	if err != nil {
		rlog.Error("Get all group list decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	return &subscriptionGroupWrapper, nil
}

func (a *admin) FetchAllTopicList(ctx context.Context) (*TopicList, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetAllTopicListFromNameServer, nil, nil)
	response, err := a.cli.InvokeSync(ctx, a.cli.GetNameSrv().AddrList()[0], cmd, 3*time.Second)
	if err != nil {
		rlog.Error("Fetch all topic list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("Fetch all topic list success", map[string]interface{}{})
	}
	var topicList TopicList
	_, err = topicList.Decode(response.Body, &topicList)
	if err != nil {
		rlog.Error("Fetch all topic list decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	return &topicList, nil
}

// Decode overrides decode method avoid the problem of server-side returned JSON **not** conforming to
// the JSON specification(JSON key should always is string, don't use int as key).
// Related Issue: https://github.com/apache/rocketmq/issues/3369
func (a *ClusterInfo) Decode(data []byte, classOfT interface{}) (interface{}, error) {
	jsonStr := utils.RectifyJsonIntKeysByChar(string(data))
	return a.FromJson(jsonStr, classOfT)
}

// GetBrokerClusterInfo Get Broker's Cluster Info, Address Table, and so on
func (a *admin) GetBrokerClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerClusterInfo, nil, nil)
	response, err := a.cli.InvokeSync(ctx, a.cli.GetNameSrv().AddrList()[0], cmd, 3*time.Second)
	if err != nil {
		rlog.Error("Fetch cluster info error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	rlog.Info("Fetch cluster info success", map[string]interface{}{})

	var clusterInfo ClusterInfo
	_, err = clusterInfo.Decode(response.Body, &clusterInfo)
	if err != nil {
		rlog.Error("Fetch cluster info decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	return &clusterInfo, nil
}

func (a *admin) checkIsTopicDuplicated(ctx context.Context, topic string) (bool, error) {
	topicList, err := a.FetchAllTopicList(ctx)
	if err != nil {
		return false, err
	}
	for _, t := range topicList.TopicList {
		if t == topic {
			return true, nil
		}
	}
	return false, nil
}

func (a *admin) FindBrokerAddrByName(ctx context.Context, brokerName string) ([]string, error) {
	var brokersAddrList []string
	clusterInfo, err := a.GetBrokerClusterInfo(ctx)
	if err != nil {
		rlog.Error("call GetBrokerClusterInfo error", map[string]interface{}{
			rlog.LogKeyBroker:        brokerName,
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	// fetch broker addr via broker name
	if brokerName != "" {
		if val, exist := clusterInfo.BrokerAddrTable[brokerName]; exist {
			// only add master broker address
			brokersAddrList = append(brokersAddrList, val.BrokerAddresses[internal.MasterId])
		} else {
			rlog.Error("create topic error", map[string]interface{}{
				rlog.LogKeyBroker:        brokerName,
				rlog.LogKeyUnderlayError: "Broker Name not found",
			})
			return nil, errors.New("create topic error due to broker name not found")
		}
	} else {
		// not given broker addr and name, then create topic on all broker of default cluster
		for _, nestedBrokerAddrData := range clusterInfo.BrokerAddrTable {
			// only add master broker address
			brokersAddrList = append(brokersAddrList, nestedBrokerAddrData.BrokerAddresses[internal.MasterId])
		}
	}
	return brokersAddrList, nil
}

// CreateTopic create topic.
// Done: another implementation like sarama, without brokerAddr as input
func (a *admin) CreateTopic(ctx context.Context, opts ...OptionCreate) error {
	cfg := defaultTopicConfigCreate()
	for _, apply := range opts {
		apply(&cfg)
	}
	if cfg.Topic == "" {
		rlog.Error("empty topic", map[string]interface{}{})
		return errors.New("topic is empty string")
	}

	if cfg.OptNotOverride {
		isExist, err := a.checkIsTopicDuplicated(ctx, cfg.Topic)
		if err != nil {
			rlog.Error("failed to FetchAllTopicList", map[string]interface{}{
				rlog.LogKeyUnderlayError: err,
			})
			return errors.New("failed to FetchAllTopicList")
		}
		if isExist {
			rlog.Error("same name topic is exist", map[string]interface{}{
				rlog.LogKeyTopic:         cfg.Topic,
				rlog.LogKeyUnderlayError: "topic is duplicated",
			})
			return errors.New("topic is duplicated")
		}
	}

	request := &internal.CreateTopicRequestHeader{
		Topic:           cfg.Topic,
		DefaultTopic:    cfg.DefaultTopic,
		ReadQueueNums:   cfg.ReadQueueNums,
		WriteQueueNums:  cfg.WriteQueueNums,
		Perm:            cfg.Perm,
		TopicFilterType: cfg.TopicFilterType,
		TopicSysFlag:    cfg.TopicSysFlag,
		Order:           cfg.Order,
	}

	cmd := remote.NewRemotingCommand(internal.ReqCreateTopic, request, nil)
	var brokersAddrList []string
	if cfg.BrokerAddr == "" {
		// we need get broker addr table from RocketMQ server
		foundBrokers, err := a.FindBrokerAddrByName(ctx, cfg.BrokerName)
		if err != nil {
			return err
		}
		if foundBrokers == nil || len(foundBrokers) == 0 {
			return errors.New("broker name not found")
		}
		brokersAddrList = append(brokersAddrList, foundBrokers...)
	} else {
		brokersAddrList = append(brokersAddrList, cfg.BrokerAddr)
	}
	var invokeErrorStrings []string
	for _, brokerAddr := range brokersAddrList {
		_, invokeErr := a.cli.InvokeSync(ctx, brokerAddr, cmd, 5*time.Second)
		if invokeErr != nil {
			invokeErrorStrings = append(invokeErrorStrings, invokeErr.Error())
			rlog.Error("create topic error", map[string]interface{}{
				rlog.LogKeyTopic:         cfg.Topic,
				rlog.LogKeyBroker:        brokerAddr,
				rlog.LogKeyUnderlayError: invokeErr,
			})
		} else {
			rlog.Info("create topic success", map[string]interface{}{
				rlog.LogKeyTopic:  cfg.Topic,
				rlog.LogKeyBroker: brokerAddr,
			})
		}
	}
	// go 1.13 not support errors.Join(err, nil, err2, err3), so we join with string repr of error
	return fmt.Errorf(strings.Join(invokeErrorStrings, "\n"))
}

// DeleteTopicInBroker delete topic in broker.
func (a *admin) deleteTopicInBroker(ctx context.Context, topic string, brokerAddr string) (*remote.RemotingCommand, error) {
	request := &internal.DeleteTopicRequestHeader{
		Topic: topic,
	}

	cmd := remote.NewRemotingCommand(internal.ReqDeleteTopicInBroker, request, nil)
	return a.cli.InvokeSync(ctx, brokerAddr, cmd, 5*time.Second)
}

// DeleteTopicInNameServer delete topic in nameserver.
func (a *admin) deleteTopicInNameServer(ctx context.Context, topic string, nameSrvAddr string) (*remote.RemotingCommand, error) {
	request := &internal.DeleteTopicRequestHeader{
		Topic: topic,
	}

	cmd := remote.NewRemotingCommand(internal.ReqDeleteTopicInNameSrv, request, nil)
	return a.cli.InvokeSync(ctx, nameSrvAddr, cmd, 5*time.Second)
}

// DeleteTopic delete topic in both broker and nameserver.
func (a *admin) DeleteTopic(ctx context.Context, opts ...OptionDelete) error {
	cfg := defaultTopicConfigDelete()
	for _, apply := range opts {
		apply(&cfg)
	}
	// delete topic in broker
	if cfg.BrokerAddr == "" {
		a.cli.GetNameSrv().UpdateTopicRouteInfo(cfg.Topic)
		cfg.BrokerAddr = a.cli.GetNameSrv().FindBrokerAddrByTopic(cfg.Topic)
	}

	if _, err := a.deleteTopicInBroker(ctx, cfg.Topic, cfg.BrokerAddr); err != nil {
		rlog.Error("delete topic in broker error", map[string]interface{}{
			rlog.LogKeyTopic:         cfg.Topic,
			rlog.LogKeyBroker:        cfg.BrokerAddr,
			rlog.LogKeyUnderlayError: err,
		})
		return err
	}

	// delete topic in nameserver
	if len(cfg.NameSrvAddr) == 0 {
		a.cli.GetNameSrv().UpdateTopicRouteInfo(cfg.Topic)
		cfg.NameSrvAddr = a.cli.GetNameSrv().AddrList()
		_, _, err := a.cli.GetNameSrv().UpdateTopicRouteInfo(cfg.Topic)
		if err != nil {
			rlog.Error("delete topic in nameserver error", map[string]interface{}{
				rlog.LogKeyTopic:         cfg.Topic,
				rlog.LogKeyUnderlayError: err,
			})
		}
		cfg.NameSrvAddr = a.cli.GetNameSrv().AddrList()
	}

	for _, nameSrvAddr := range cfg.NameSrvAddr {
		if _, err := a.deleteTopicInNameServer(ctx, cfg.Topic, nameSrvAddr); err != nil {
			rlog.Error("delete topic in nameserver error", map[string]interface{}{
				"nameServer":             nameSrvAddr,
				rlog.LogKeyTopic:         cfg.Topic,
				rlog.LogKeyUnderlayError: err,
			})
			return err
		}
	}
	rlog.Info("delete topic success", map[string]interface{}{
		"nameServer":      cfg.NameSrvAddr,
		rlog.LogKeyTopic:  cfg.Topic,
		rlog.LogKeyBroker: cfg.BrokerAddr,
	})
	return nil
}

func (a *admin) FetchPublishMessageQueues(ctx context.Context, topic string) ([]*primitive.MessageQueue, error) {
	return a.cli.GetNameSrv().FetchPublishMessageQueues(utils.WrapNamespace(a.opts.Namespace, topic))
}

func (a *admin) FetchClusterList(topic string) ([]string, error) {
	return a.cli.GetNameSrv().FetchClusterList(topic)
}

func (a *admin) Close() error {
	a.closeOnce.Do(func() {
		a.cli.Shutdown()
	})
	return nil
}
