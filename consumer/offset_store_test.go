package consumer

import (
	"github.com/apache/rocketmq-client-go/primitive"
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
					group:       "testGroup",
					path:        _LocalOffsetStorePath + "/testGroup/offset.json",
					OffsetTable: map[string]map[int]*queueOffset{},
				},
			}, {
				clientId: "192.168.24.1@default",
				group:    "",
				expectedResult: &localFileOffsetStore{
					group:       "",
					path:        _LocalOffsetStorePath + "/192.168.24.1@default/offset.json",
					OffsetTable: map[string]map[int]*queueOffset{},
				},
			}, {
				clientId: "192.168.24.1@default",
				group:    "testGroup",
				expectedResult: &localFileOffsetStore{
					group:       "testGroup",
					path:        _LocalOffsetStorePath + "/192.168.24.1@default/testGroup/offset.json",
					OffsetTable: map[string]map[int]*queueOffset{},
				},
			},
		}

		for _, value := range cases {
			result := NewLocalFileOffsetStore(value.clientId, value.group)
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

func TestLocalOffset(t *testing.T) {
	Convey("Given a local store with a starting value", t, func() {
		clientId := "192.168.24.1@default"
		group := "testGroup"
		local := NewLocalFileOffsetStore(clientId, group)

		type offsetCase struct {
			setOffset      int64
			expectedOffset int64
		}
		queues := []*primitive.MessageQueue{
			{
				Topic:      "testTopic1",
				BrokerName: "default",
				QueueId:    1,
			},
			{
				Topic:      "testTopic2",
				BrokerName: "default",
				QueueId:    2,
			},
		}

		Convey("test update", func() {
			Convey("when increaseOnly is false", func() {
				cases := []offsetCase{
					{
						setOffset:      3,
						expectedOffset: 3,
					}, {
						setOffset:      1,
						expectedOffset: 1,
					},
				}
				for _, value := range cases {
					local.update(queues[0], value.setOffset, false)
					offset := local.read(queues[0], _ReadFromMemory)
					So(offset, ShouldEqual, value.expectedOffset)
				}
			})

			Convey("when increaseOnly is true", func() {
				local.update(queues[0], 0, false)

				cases := []offsetCase{
					{
						setOffset:      3,
						expectedOffset: 3,
					}, {
						setOffset:      1,
						expectedOffset: 3,
					},
				}
				for _, value := range cases {
					local.update(queues[0], value.setOffset, true)
					offset := local.read(queues[0], _ReadFromMemory)
					So(offset, ShouldEqual, value.expectedOffset)
				}
			})
		})

		Convey("test persist", func() {
			local.update(queues[0], 1, false)

			local.persist(queues)
			offset := local.read(queues[0], _ReadFromStore)
			So(offset, ShouldEqual, 1)
		})
	})
}

