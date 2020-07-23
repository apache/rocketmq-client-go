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

func defaultTopicConfigCreate() TopicConfigCreate {
	opts := TopicConfigCreate{
		DefaultTopic:    "defaultTopic",
		ReadQueueNums:   8,
		WriteQueueNums:  8,
		Perm:            6,
		TopicFilterType: "SINGLE_TAG",
		TopicSysFlag:    0,
		Order:           false,
	}
	return opts
}

type TopicConfigCreate struct {
	Topic           string
	BrokerAddr      string
	DefaultTopic    string
	ReadQueueNums   int
	WriteQueueNums  int
	Perm            int
	TopicFilterType string
	TopicSysFlag    int
	Order           bool
}

type OptionCreate func(*TopicConfigCreate)

func WithTopicCreate(Topic string) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.Topic = Topic
	}
}

func WithBrokerAddrCreate(BrokerAddr string) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.BrokerAddr = BrokerAddr
	}
}

func WithReadQueueNums(ReadQueueNums int) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.ReadQueueNums = ReadQueueNums
	}
}

func WithWriteQueueNums(WriteQueueNums int) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.WriteQueueNums = WriteQueueNums
	}
}

func WithPerm(Perm int) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.Perm = Perm
	}
}

func WithTopicFilterType(TopicFilterType string) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.TopicFilterType = TopicFilterType
	}
}

func WithTopicSysFlag(TopicSysFlag int) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.TopicSysFlag = TopicSysFlag
	}
}

func WithOrder(Order bool) OptionCreate {
	return func(opts *TopicConfigCreate) {
		opts.Order = Order
	}
}

func defaultTopicConfigDelete() TopicConfigDelete {
	opts := TopicConfigDelete{}
	return opts
}

type TopicConfigDelete struct {
	Topic       string
	ClusterName string
	NameSrvAddr []string
	BrokerAddr  string
}

type OptionDelete func(*TopicConfigDelete)

func WithTopicDelete(Topic string) OptionDelete {
	return func(opts *TopicConfigDelete) {
		opts.Topic = Topic
	}
}

func WithBrokerAddrDelete(BrokerAddr string) OptionDelete {
	return func(opts *TopicConfigDelete) {
		opts.BrokerAddr = BrokerAddr
	}
}

func WithClusterName(ClusterName string) OptionDelete {
	return func(opts *TopicConfigDelete) {
		opts.ClusterName = ClusterName
	}
}

func WithNameSrvAddr(NameSrvAddr []string) OptionDelete {
	return func(opts *TopicConfigDelete) {
		opts.NameSrvAddr = NameSrvAddr
	}
}
