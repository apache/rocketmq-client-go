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

	"github.com/apache/rocketmq-client-go/internal/utils"
	"github.com/apache/rocketmq-client-go/rlog"
)

type FindBrokerResult struct {
	BrokerAddr    string
	Slave         bool
	BrokerVersion int32
}

type (
	// groupName of consumer
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
	ClassFilterMode bool      `json:"classFilterMode"`
	Topic           string    `json:"topic"`
	SubString       string    `json:"subString"`
	Tags            utils.Set `json:"tagsSet"`
	Codes           utils.Set `json:"codeSet"`
	SubVersion      int64     `json:"subVersion"`
	ExpType         string    `json:"expressionType"`
}

type producerData struct {
	GroupName string `json:"groupName"`
}

func (p producerData) UniqueID() string {
	return p.GroupName
}

type consumerData struct {
	GroupName         string              `json:"groupName"`
	CType             consumeType         `json:"consumeType"`
	MessageModel      string              `json:"messageModel"`
	Where             string              `json:"consumeFromWhere"`
	SubscriptionDatas []*SubscriptionData `json:"subscriptionDataSet"`
	UnitMode          bool                `json:"unitMode"`
}

func (c consumerData) UniqueID() string {
	return c.GroupName
}

type heartbeatData struct {
	ClientId      string    `json:"clientID"`
	ProducerDatas utils.Set `json:"producerDataSet"`
	ConsumerDatas utils.Set `json:"consumerDataSet"`
}

func NewHeartbeatData(clientID string) *heartbeatData {
	return &heartbeatData{
		ClientId:      clientID,
		ProducerDatas: utils.NewSet(),
		ConsumerDatas: utils.NewSet(),
	}
}

func (data *heartbeatData) encode() []byte {
	d, err := json.Marshal(data)
	if err != nil {
		rlog.Error("marshal heartbeatData error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil
	}
	rlog.Debug("heartbeat: "+string(d), nil)
	return d
}
