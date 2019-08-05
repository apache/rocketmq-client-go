package consumer

import (
	"github.com/agiledragon/gomonkey"
	"github.com/apache/rocketmq-client-go/internal"
	"github.com/apache/rocketmq-client-go/internal/remote"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestNewLocalFileOffsetStore(t *testing.T) {
	Convey("Given some test cases", t, func() {
		type testCase struct {
			clientId       string
			group          string
			expectedResult *localFileOffsetStore
		}
		cases := []testCase{
			{
				clientId: "",
				group:    "testGroup",
				expectedResult: &localFileOffsetStore{
					group: "testGroup",
					path:  _LocalOffsetStorePath + "/testGroup/offset.json",
				},
			}, {
				clientId: "192.168.24.1@default",
				group:    "",
				expectedResult: &localFileOffsetStore{
					group: "",
					path:  _LocalOffsetStorePath + "/192.168.24.1@default/offset.json",
				},
			}, {
				clientId: "192.168.24.1@default",
				group:    "testGroup",
				expectedResult: &localFileOffsetStore{
					group: "testGroup",
					path:  _LocalOffsetStorePath + "/192.168.24.1@default/testGroup/offset.json",
				},
			},
		}

		for _, value := range cases {
			result := NewLocalFileOffsetStore(value.clientId, value.group).(*localFileOffsetStore)
			value.expectedResult.OffsetTable = result.OffsetTable
			So(result, ShouldResemble, value.expectedResult)
		}
	})
}

func TestReadFromMemory(t *testing.T) {
	Convey("Given some queue and queueOffset with a starting value", t, func() {
		type testCase struct {
			queueOffset    map[string]map[int]*queueOffset
			queue          *primitive.MessageQueue
			expectedOffset int64
		}

		cases := []testCase{
			{
				queueOffset: map[string]map[int]*queueOffset{
					"testTopic2": {
						1: {
							QueueID: 1,
							Broker:  "default",
							Offset:  1,
						},
					},
				},
				queue: &primitive.MessageQueue{
					Topic:      "testTopic1",
					BrokerName: "default",
					QueueId:    1,
				},
				expectedOffset: -1,
			},
			{
				queueOffset: map[string]map[int]*queueOffset{
					"testTopic1": {
						0: {
							QueueID: 0,
							Broker:  "default",
							Offset:  1,
						},
					},
				},
				queue: &primitive.MessageQueue{
					Topic:      "testTopic1",
					BrokerName: "default",
					QueueId:    1,
				},
				expectedOffset: -1,
			},
			{
				queueOffset: map[string]map[int]*queueOffset{
					"testTopic1": {
						0: {
							QueueID: 0,
							Broker:  "default",
							Offset:  1,
						},
					},
				},
				queue: &primitive.MessageQueue{
					Topic:      "testTopic1",
					BrokerName: "default",
					QueueId:    0,
				},
				expectedOffset: 1,
			},
		}

		for _, value := range cases {
			offset := readFromMemory(value.queueOffset, value.queue)
			So(offset, ShouldEqual, value.expectedOffset)
		}
	})
}

func TestLocalFileOffsetStore(t *testing.T) {
	Convey("Given a local store with a starting value", t, func() {
		localStore := NewLocalFileOffsetStore("192.168.24.1@default", "testGroup")

		type offsetCase struct {
			queue          *primitive.MessageQueue
			setOffset      int64
			expectedOffset int64
		}
		mq := &primitive.MessageQueue{
			Topic:      "testTopic",
			BrokerName: "default",
			QueueId:    1,
		}

		Convey("test update", func() {
			Convey("when increaseOnly is false", func() {
				cases := []offsetCase{
					{
						queue:          mq,
						setOffset:      3,
						expectedOffset: 3,
					}, {
						queue:          mq,
						setOffset:      1,
						expectedOffset: 1,
					},
				}
				for _, value := range cases {
					localStore.update(value.queue, value.setOffset, false)
					offset := localStore.read(value.queue, _ReadFromMemory)
					So(offset, ShouldEqual, value.expectedOffset)
				}
			})

			Convey("when increaseOnly is true", func() {
				localStore.update(mq, 0, false)

				cases := []offsetCase{
					{
						queue:          mq,
						setOffset:      3,
						expectedOffset: 3,
					}, {
						queue:          mq,
						setOffset:      1,
						expectedOffset: 3,
					},
				}
				for _, value := range cases {
					localStore.update(value.queue, value.setOffset, true)
					offset := localStore.read(value.queue, _ReadFromMemory)
					So(offset, ShouldEqual, value.expectedOffset)
				}
			})
		})

		Convey("test persist", func() {
			localStore.update(mq, 1, false)

			queues := []*primitive.MessageQueue{mq}
			localStore.persist(queues)
			offset := localStore.read(mq, _ReadFromStore)
			So(offset, ShouldEqual, 1)
		})
	})
}

func TestRemoteBrokerOffsetStore(t *testing.T) {
	Convey("Given a remote store with a starting value", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		rmqClient := internal.NewMockRMQClient(ctrl)
		remoteStore := NewRemoteOffsetStore("testGroup", rmqClient)

		type offsetCase struct {
			queue          *primitive.MessageQueue
			setOffset      int64
			expectedOffset int64
		}
		mq := &primitive.MessageQueue{
			Topic:      "testTopic",
			BrokerName: "default",
			QueueId:    1,
		}

		Convey("test update", func() {
			Convey("when increaseOnly is false", func() {
				cases := []offsetCase{
					{
						queue:          mq,
						setOffset:      3,
						expectedOffset: 3,
					}, {
						queue:          mq,
						setOffset:      1,
						expectedOffset: 1,
					},
				}
				for _, value := range cases {
					remoteStore.update(value.queue, value.setOffset, false)
					offset := remoteStore.read(value.queue, _ReadFromMemory)
					So(offset, ShouldEqual, value.expectedOffset)
				}
			})

			Convey("when increaseOnly is true", func() {
				remoteStore.update(mq, 0, false)

				cases := []offsetCase{
					{
						queue:          mq,
						setOffset:      3,
						expectedOffset: 3,
					}, {
						queue:          mq,
						setOffset:      1,
						expectedOffset: 3,
					},
				}
				for _, value := range cases {
					remoteStore.update(value.queue, value.setOffset, true)
					offset := remoteStore.read(value.queue, _ReadFromMemory)
					So(offset, ShouldEqual, value.expectedOffset)
				}
			})
		})

		Convey("test persist", func() {
			queues := []*primitive.MessageQueue{mq}

			patch := gomonkey.ApplyFunc(internal.FindBrokerAddrByName, func(_ string) (string) {
				return "192.168.24.1:10911"
			})
			defer patch.Reset()

			ret := &remote.RemotingCommand{
				Code: internal.ResSuccess,
				ExtFields: map[string]string{
					"offset": "1",
				},
			}
			rmqClient.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any()).Return(ret, nil)

			remoteStore.persist(queues)
			offset := remoteStore.read(mq, _ReadFromStore)
			So(offset, ShouldEqual, 1)
		})

		Convey("test remove", func() {
			remoteStore.update(mq, 1, false)
			offset := remoteStore.read(mq, _ReadFromMemory)
			So(offset, ShouldEqual, 1)

			remoteStore.remove(mq)
			offset = remoteStore.read(mq, _ReadFromMemory)
			So(offset, ShouldEqual, -1)
		})
	})
}
