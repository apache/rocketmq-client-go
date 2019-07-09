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
	"encoding/json"

	"github.com/apache/rocketmq-client-go/rlog"
)

type FindBrokerResult struct {
	BrokerAddr    string
	Slave         bool
	BrokerVersion int32
}

type (
	// groupName of consumer
	producerData string

	consumeType string

	ServiceState int
)

const (
	StateCreateJust ServiceState = iota
	StateStartFailed
	StateRunning
	StateShutdown
)

type SubscriptionData struct {
	ClassFilterMode bool
	Topic           string
	SubString       string
	Tags            map[string]bool
	Codes           map[int32]bool
	SubVersion      int64
	ExpType         string
}

type consumerData struct {
	GroupName         string              `json:"groupName"`
	CType             consumeType         `json:"consumeType"`
	MessageModel      string              `json:"messageModel"`
	Where             string              `json:"consumeFromWhere"`
	SubscriptionDatas []*SubscriptionData `json:"subscriptionDataSet"`
	UnitMode          bool                `json:"unitMode"`
}

type heartbeatData struct {
	ClientId      string         `json:"clientID"`
	ProducerDatas []producerData `json:"producerDataSet"`
	ConsumerDatas []consumerData `json:"consumerDataSet"`
}

func (data *heartbeatData) encode() []byte {
	d, err := json.Marshal(data)
	if err != nil {
		rlog.Errorf("marshal heartbeatData error: %s", err.Error())
		return nil
	}
	rlog.Info(string(d))
	return d
}
