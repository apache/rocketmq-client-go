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

package consumer

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestPullFromQueue(t *testing.T) {
	Convey("ManualPullConsumer PullFromQueue", t, func() {
		serverTopic := "foo"
		tests := map[string]struct {
			mq          primitive.MessageQueue
			offset      int64
			numbers     int
			expectedErr bool
		}{
			"topic exist": {
				primitive.MessageQueue{
					Topic:      "foo",
					BrokerName: "",
					QueueId:    0,
				},
				1,
				1,
				false,
			},
			"topic not exist": {
				primitive.MessageQueue{
					Topic:      "foo2",
					BrokerName: "",
					QueueId:    0,
				},
				1,
				1,
				false,
			},
		}

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c, err := NewManualPullConsumer(WithNameServer(primitive.NamesrvAddr{"127.0.0.1"}))
		if err != nil {
			assert.Error(t, err)
		}

		namesrv, client := internal.NewMockNamesrvs(ctrl), internal.NewMockRMQClient(ctrl)
		c.namesrv, c.client = namesrv, client
		namesrv.EXPECT().FindBrokerAddressInSubscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(
			&internal.FindBrokerResult{
				BrokerAddr:    "foo",
				Slave:         false,
				BrokerVersion: 1.0,
			},
		)
		client.EXPECT().PullMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, brokerAddrs string, request *internal.PullMessageRequestHeader) (*primitive.PullResult, error) {
				pullResult := &primitive.PullResult{
					SuggestWhichBrokerId: 0,
				}
				if request.Topic == serverTopic {
					pullResult.Status = primitive.PullFound
				} else {
					pullResult.Status = primitive.PullNoNewMsg
				}
				return pullResult, nil
			})

		for name, test := range tests {
			Convey(name, func() {
				_, err := c.PullFromQueue(context.Background(), "default", &test.mq, test.offset, test.numbers)
				So(err, ShouldBeNil)
			})
		}

	})
}

func TestGetMessageQueues(t *testing.T) {
	Convey("ManualPullConsumer GetMessageQueues", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c, err := NewManualPullConsumer(WithNameServer(primitive.NamesrvAddr{"127.0.0.1"}))
		if err != nil {
			assert.Error(t, err)
		}

		namesrv := internal.NewMockNamesrvs(ctrl)
		c.namesrv = namesrv
		namesrv.EXPECT().FetchSubscribeMessageQueues("foo").Return(
			[]*primitive.MessageQueue{
				{Topic: "foo", BrokerName: "foo", QueueId: 0},
			}, nil,
		)

		queues, err := c.GetMessageQueues(context.TODO(), "foo")

		So(queues, ShouldNotBeEmpty)
		So(err, ShouldBeNil)
	})
}

func TestCommittedOffset(t *testing.T) {
	Convey("ManualPullConsumer CommittedOffset", t, func() {

		serverTopic, serverOffset := "foo", "1"
		tests := map[string]struct {
			mq          primitive.MessageQueue
			except      int
			expectedErr bool
		}{
			"topic exist": {
				primitive.MessageQueue{
					Topic:      "foo",
					BrokerName: "",
					QueueId:    0,
				},
				1,
				false,
			},
			"topic not exist": {
				primitive.MessageQueue{
					Topic:      "foo2",
					BrokerName: "",
					QueueId:    0,
				},
				-2,
				true,
			},
		}

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c, err := NewManualPullConsumer(WithNameServer(primitive.NamesrvAddr{"127.0.0.1"}))
		if err != nil {
			assert.Error(t, err)
		}
		namesrv, client := internal.NewMockNamesrvs(ctrl), internal.NewMockRMQClient(ctrl)
		c.namesrv, c.client = namesrv, client

		namesrv.EXPECT().FindBrokerAddrByName(gomock.Any()).Return("foo").AnyTimes()
		client.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, addr string, request *remote.RemotingCommand, timeoutMillis time.Duration) (*remote.RemotingCommand, error) {
				if request.ExtFields["topic"] == serverTopic {
					ret := &remote.RemotingCommand{
						Code: internal.ResSuccess,
						ExtFields: map[string]string{
							"offset": serverOffset,
						},
					}
					return ret, nil
				}
				ret := &remote.RemotingCommand{
					Code: internal.ResTopicNotExist,
				}
				return ret, nil
			}).AnyTimes()

		for name, test := range tests {
			Convey(name, func() {
				ret, err := c.CommittedOffset(context.Background(), "foo", &test.mq)

				if test.expectedErr {
					So(err, ShouldNotBeNil)
				} else {
					So(err, ShouldBeNil)
				}
				So(ret, ShouldEqual, test.except)
			})
		}
	})
}

func TestSeek(t *testing.T) {
	Convey("ManualPullConsumer Seek", t, func() {

		serverMinOffset, serverMaxOffset := "1", "10"
		tests := map[string]struct {
			mq          primitive.MessageQueue
			offset      int64
			expectedErr bool
		}{
			"normal offset": {
				primitive.MessageQueue{
					Topic:      "foo",
					BrokerName: "foo",
					QueueId:    0,
				},
				3,
				false,
			},
			"illegal offset": {
				primitive.MessageQueue{
					Topic:      "foo",
					BrokerName: "foo",
					QueueId:    0,
				},
				11,
				true,
			},
		}

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c, err := NewManualPullConsumer(WithNameServer(primitive.NamesrvAddr{"127.0.0.1"}))
		if err != nil {
			assert.Error(t, err)
		}
		namesrv, client := internal.NewMockNamesrvs(ctrl), internal.NewMockRMQClient(ctrl)
		c.namesrv, c.client = namesrv, client

		namesrv.EXPECT().FindBrokerAddrByName(gomock.Any()).Return("foo").AnyTimes()

		client.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, addr string, request *remote.RemotingCommand, timeoutMillis time.Duration) (*remote.RemotingCommand, error) {
				if request.Code == internal.ReqGetMinOffset {
					return &remote.RemotingCommand{
						Code: internal.ResSuccess,
						ExtFields: map[string]string{
							"offset": serverMinOffset,
						},
					}, nil
				} else if request.Code == internal.ReqGetMaxOffset {
					return &remote.RemotingCommand{
						Code: internal.ResSuccess,
						ExtFields: map[string]string{
							"offset": serverMaxOffset,
						},
					}, nil
				}
				return &remote.RemotingCommand{
					Code: internal.ResError,
				}, nil
			}).AnyTimes()

		client.EXPECT().InvokeOneWay(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		for name, test := range tests {
			Convey(name, func() {
				err := c.Seek(context.Background(), "foo", &test.mq, test.offset)
				if test.expectedErr {
					So(err, ShouldNotBeNil)
				} else {
					So(err, ShouldBeNil)
				}
			})
		}
	})
}

func TestLookup(t *testing.T) {
	Convey("ManualPullConsumer Lookup", t, func() {
		test := struct {
			mq          primitive.MessageQueue
			timestamp   int64
			offset      int64
			expectedErr bool
		}{
			primitive.MessageQueue{
				Topic:      "foo",
				BrokerName: "foo",
				QueueId:    0,
			},
			int64(time.Now().Nanosecond()),
			10,
			true,
		}

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c, err := NewManualPullConsumer(WithNameServer(primitive.NamesrvAddr{"127.0.0.1"}))
		if err != nil {
			assert.Error(t, err)
		}
		namesrv, client := internal.NewMockNamesrvs(ctrl), internal.NewMockRMQClient(ctrl)
		c.namesrv, c.client = namesrv, client

		namesrv.EXPECT().FindBrokerAddrByName(gomock.Any()).Return("foo").AnyTimes()

		client.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, addr string, request *remote.RemotingCommand, timeoutMillis time.Duration) (*remote.RemotingCommand, error) {

				return &remote.RemotingCommand{
					Code: internal.ResSuccess,
					ExtFields: map[string]string{
						"offset": strconv.FormatInt(test.offset, 10),
					},
				}, nil
			}).AnyTimes()

		ret, err := c.Lookup(context.Background(), &test.mq, test.timestamp)
		So(err, ShouldBeNil)
		So(ret, ShouldEqual, test.offset)
	})
}

func TestShutdown(t *testing.T) {
	Convey("ManualPullConsumer Shutdown", t, func() {

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c, err := NewManualPullConsumer(WithNameServer(primitive.NamesrvAddr{"127.0.0.1"}))
		if err != nil {
			assert.Error(t, err)
		}
		client := internal.NewMockRMQClient(ctrl)
		c.client = client

		client.EXPECT().Shutdown().Times(1)
		c.Shutdown()
		c.Shutdown()
	})
}
