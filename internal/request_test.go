package internal

import (
	"github.com/smartystreets/assertions/should"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestCheckTransactionStateRequestHeader_Decode(t *testing.T) {
	request := &CheckTransactionStateRequestHeader{}
	Convey("test CheckTransactionStateRequestHeader Decode method", t, func() {
		Convey("test properties success", func() {
			properties := map[string]string{
				"tranStateTableOffset": "10",
				"commitLogOffset":      "10",
				"msgId":                "msgIdValue",
				"transactionId":        "transactionIdValue",
				"offsetMsgId":          "offsetMsgIdValue",
			}
			request.Decode(properties)
		})
		Convey("test properties length equals 0", func() {
			properties := map[string]string{}
			request.Decode(properties)
		})
	})
}

func TestCheckTransactionStateRequestHeader_Encode(t *testing.T) {
	request := &CheckTransactionStateRequestHeader{
		TranStateTableOffset: 10,
		CommitLogOffset:      10,
		MsgId:                "msgIdValue",
		TransactionId:        "transactionIdValue",
		OffsetMsgId:          "offsetMsgIdValue",
	}
	properties := map[string]string{
		"tranStateTableOffset": "10",
		"commitLogOffset":      "10",
		"msgId":                "msgIdValue",
		"transactionId":        "transactionIdValue",
		"offsetMsgId":          "offsetMsgIdValue",
	}
	Convey("test CheckTransactionStateRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestConsumeMessageDirectlyHeader_Decode(t *testing.T) {
	request := &ConsumeMessageDirectlyHeader{}
	Convey("test ConsumeMessageDirectlyHeader Decode method", t, func() {
		Convey("test properties success", func() {
			properties := map[string]string{
				"consumerGroup": "consumerGroupValue",
				"clientId":      "clientIdValue",
				"msgId":         "msgIdValue",
				"brokerName":    "brokerNameValue",
			}
			request.Decode(properties)
		})
		Convey("test properties length equals 0", func() {
			properties := map[string]string{}
			request.Decode(properties)
		})
	})
}

func TestConsumeMessageDirectlyHeader_Encode(t *testing.T) {
	request := &ConsumeMessageDirectlyHeader{
		consumerGroup: "consumerGroupValue",
		clientID:      "clientIdValue",
		msgId:         "msgIdValue",
		brokerName:    "brokerNameValue",
	}
	properties := map[string]string{
		"consumerGroup": "consumerGroupValue",
		"clientId":      "clientIdValue",
		"msgId":         "msgIdValue",
		"brokerName":    "brokerNameValue",
	}
	Convey("test ConsumeMessageDirectlyHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestConsumerSendMsgBackRequestHeader_Encode(t *testing.T) {
	request := &ConsumerSendMsgBackRequestHeader{
		Group:             "groupValue",
		Offset:            10,
		DelayLevel:        10,
		OriginMsgId:       "originMsgIdValue",
		OriginTopic:       "originTopicValue",
		UnitMode:          false,
		MaxReconsumeTimes: 10,
	}
	properties := map[string]string{
		"group":             "groupValue",
		"offset":            "10",
		"delayLevel":        "10",
		"originMsgId":       "originMsgIdValue",
		"originTopic":       "originTopicValue",
		"unitMode":          "false",
		"maxReconsumeTimes": "10",
	}
	Convey("test ConsumerSendMsgBackRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestCreateTopicRequestHeader_Encode(t *testing.T) {
	request := &CreateTopicRequestHeader{
		Topic:           "topicValue",
		DefaultTopic:    "defaultTopicValue",
		ReadQueueNums:   10,
		WriteQueueNums:  10,
		Perm:            10,
		TopicFilterType: "topicFilterTypeValue",
		TopicSysFlag:    10,
		Order:           false,
	}
	properties := map[string]string{
		"topic":           "topicValue",
		"defaultTopic":    "defaultTopicValue",
		"readQueueNums":   "10",
		"writeQueueNums":  "10",
		"perm":            "10",
		"topicFilterType": "topicFilterTypeValue",
		"topicSysFlag":    "10",
		"order":           "false",
	}
	Convey("test CreateTopicRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestDeleteTopicRequestHeader_Encode(t *testing.T) {
	request := &DeleteTopicRequestHeader{
		Topic: "topicValue",
	}
	properties := map[string]string{
		"topic": "topicValue",
	}
	Convey("test DeleteTopicRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestEndTransactionRequestHeader_Encode(t *testing.T) {
	request := &EndTransactionRequestHeader{
		ProducerGroup:        "producerGroupValue",
		TranStateTableOffset: 10,
		CommitLogOffset:      10,
		CommitOrRollback:     10,
		FromTransactionCheck: false,
		MsgID:                "msgIdValue",
		TransactionId:        "transactionIdValue",
	}
	properties := map[string]string{
		"producerGroup":        "producerGroupValue",
		"tranStateTableOffset": "10",
		"commitLogOffset":      "10",
		"commitOrRollback":     "10",
		"fromTransactionCheck": "false",
		"msgId":                "msgIdValue",
		"transactionId":        "transactionIdValue",
	}
	Convey("test EndTransactionRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestGetConsumerListRequestHeader_Encode(t *testing.T) {
	request := &GetConsumerListRequestHeader{
		ConsumerGroup: "consumerGroupValue",
	}
	properties := map[string]string{
		"consumerGroup": "consumerGroupValue",
	}
	Convey("test GetConsumerListRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestGetConsumerRunningInfoHeader_Decode(t *testing.T) {
	request := &GetConsumerRunningInfoHeader{}
	Convey("test GetConsumerRunningInfoHeader Decode method", t, func() {
		Convey("test properties success", func() {
			properties := map[string]string{
				"consumerGroup": "consumerGroupValue",
				"clientId":      "clientIdValue",
				"jstackEnable":  "false",
			}
			request.Decode(properties)
		})
		Convey("test properties length equals 0", func() {
			properties := map[string]string{}
			request.Decode(properties)
		})
	})
}

func TestGetConsumerRunningInfoHeader_Encode(t *testing.T) {
	request := &GetConsumerRunningInfoHeader{
		consumerGroup: "consumerGroupValue",
		clientID:      "clientIdValue",
		jstackEnable:  false,
	}
	properties := map[string]string{
		"consumerGroup": "consumerGroupValue",
		"clientId":      "clientIdValue",
		"jstackEnable":  "false",
	}
	Convey("test GetConsumerRunningInfoHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestGetConsumerStatusRequestHeader_Decode(t *testing.T) {
	request := &GetConsumerStatusRequestHeader{}
	Convey("test GetConsumerStatusRequestHeader Decode method", t, func() {
		Convey("test properties success", func() {
			properties := map[string]string{
				"topic":      "topicValue",
				"group":      "groupValue",
				"clientAddr": "clientAddrValue",
			}
			request.Decode(properties)
		})
		Convey("test properties length equals 0", func() {
			properties := map[string]string{}
			request.Decode(properties)
		})
	})
}

func TestGetConsumerStatusRequestHeader_Encode(t *testing.T) {
	request := &GetConsumerStatusRequestHeader{
		topic:      "topicValue",
		group:      "groupValue",
		clientAddr: "clientAddrValue",
	}
	properties := map[string]string{
		"topic":      "topicValue",
		"group":      "groupValue",
		"clientAddr": "clientAddrValue",
	}
	Convey("test GetConsumerStatusRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestGetMaxOffsetRequestHeader_Encode(t *testing.T) {
	request := &GetMaxOffsetRequestHeader{
		Topic:   "topicValue",
		QueueId: 10,
	}
	properties := map[string]string{
		"topic":   "topicValue",
		"queueId": "10",
	}
	Convey("test GetMaxOffsetRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestGetRouteInfoRequestHeader_Encode(t *testing.T) {
	request := &GetRouteInfoRequestHeader{
		Topic: "topicValue",
	}
	properties := map[string]string{
		"topic": "topicValue",
	}
	Convey("test GetRouteInfoRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestPullMessageRequestHeader_Encode(t *testing.T) {
	request := &PullMessageRequestHeader{
		ConsumerGroup:        "consumerGroupValue",
		Topic:                "topicValue",
		QueueId:              10,
		QueueOffset:          10,
		MaxMsgNums:           10,
		SysFlag:              10,
		CommitOffset:         10,
		SuspendTimeoutMillis: 10 * time.Millisecond,
		SubExpression:        "subscriptionValue",
		SubVersion:           10,
		ExpressionType:       "expressionTypeValue",
	}
	properties := map[string]string{
		"consumerGroup":        "consumerGroupValue",
		"topic":                "topicValue",
		"queueId":              "10",
		"queueOffset":          "10",
		"maxMsgNums":           "10",
		"sysFlag":              "10",
		"commitOffset":         "10",
		"suspendTimeoutMillis": "10",
		"subscription":         "subscriptionValue",
		"subVersion":           "10",
		"expressionType":       "expressionTypeValue",
	}
	Convey("test PullMessageRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestQueryConsumerOffsetRequestHeader_Encode(t *testing.T) {
	request := &QueryConsumerOffsetRequestHeader{
		ConsumerGroup: "consumerGroupValue",
		Topic:         "topicValue",
		QueueId:       10,
	}
	properties := map[string]string{
		"consumerGroup": "consumerGroupValue",
		"topic":         "topicValue",
		"queueId":       "10",
	}
	Convey("test QueryConsumerOffsetRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestQueryMessageRequestHeader_Decode(t *testing.T) {
	request := &QueryMessageRequestHeader{}
	Convey("test QueryMessageRequestHeader Decode method", t, func() {
		properties := map[string]string{}
		err := request.Decode(properties)
		So(err, should.BeNil)
	})
}

func TestQueryMessageRequestHeader_Encode(t *testing.T) {
	request := &QueryMessageRequestHeader{
		Topic:          "topicValue",
		Key:            "keyValue",
		MaxNum:         10,
		BeginTimestamp: 10,
		EndTimestamp:   10,
	}
	properties := map[string]string{
		"topic":          "topicValue",
		"key":            "keyValue",
		"maxNum":         "10",
		"beginTimestamp": "10",
		"endTimestamp":   "10",
	}
	Convey("test QueryMessageRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestReplyMessageRequestHeader_Decode(t *testing.T) {
	request := &ReplyMessageRequestHeader{}
	Convey("test ReplyMessageRequestHeader Decode method", t, func() {
		Convey("test properties success", func() {
			properties := map[string]string{
				"producerGroup":         "producerGroupValue",
				"topic":                 "topicValue",
				"defaultTopic":          "defaultTopicValue",
				"defaultTopicQueueNums": "10",
				"queueId":               "10",
				"sysFlag":               "10",
				"bornTimestamp":         "10",
				"flag":                  "10",
				"properties":            "propertiesValue",
				"reconsumeTimes":        "10",
				"bornHost":              "bornHostValue",
				"storeHost":             "storeHostValue",
				"storeTimestamp":        "10",
			}
			request.Decode(properties)
		})
		Convey("test properties length equals 0", func() {
			properties := map[string]string{}
			request.Decode(properties)
		})
	})
}

func TestReplyMessageRequestHeader_Encode(t *testing.T) {
	request := &ReplyMessageRequestHeader{
		producerGroup:         "producerGroupValue",
		topic:                 "topicValue",
		defaultTopic:          "defaultTopicValue",
		defaultTopicQueueNums: 10,
		queueId:               10,
		sysFlag:               10,
		bornTimestamp:         10,
		flag:                  10,
		properties:            "propertiesValue",
		reconsumeTimes:        10,
		unitMode:              false,
		bornHost:              "bornHostValue",
		storeHost:             "storeHostValue",
		storeTimestamp:        10,
	}
	properties := map[string]string{
		"producerGroup":         "producerGroupValue",
		"topic":                 "topicValue",
		"defaultTopic":          "defaultTopicValue",
		"defaultTopicQueueNums": "10",
		"queueId":               "10",
		"sysFlag":               "10",
		"bornTimestamp":         "10",
		"flag":                  "10",
		"properties":            "propertiesValue",
		"reconsumeTimes":        "10",
		"bornHost":              "bornHostValue",
		"storeHost":             "storeHostValue",
		"storeTimestamp":        "10",
	}
	Convey("test ReplyMessageRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestResetOffsetHeader_Decode(t *testing.T) {
	request := &ResetOffsetHeader{}
	Convey("test ResetOffsetHeader Decode method", t, func() {
		Convey("test properties success", func() {
			properties := map[string]string{
				"topic":     "topicValue",
				"group":     "groupValue",
				"timestamp": "10",
			}
			request.Decode(properties)
		})
		Convey("test properties length equals 0", func() {
			properties := map[string]string{}
			request.Decode(properties)
		})
	})
}

func TestResetOffsetHeader_Encode(t *testing.T) {
	request := &ResetOffsetHeader{
		topic:     "topicValue",
		group:     "groupValue",
		timestamp: 10,
		isForce:   false,
	}
	properties := map[string]string{
		"topic":     "topicValue",
		"group":     "groupValue",
		"timestamp": "10",
	}
	Convey("test ResetOffsetHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestSearchOffsetRequestHeader_Encode(t *testing.T) {
	request := &SearchOffsetRequestHeader{
		Topic:     "topicValue",
		QueueId:   10,
		Timestamp: 10,
	}
	properties := map[string]string{
		"topic":     "topicValue",
		"queueId":   "10",
		"timestamp": "10",
	}
	Convey("test SearchOffsetRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestSendMessageRequestHeader_Encode(t *testing.T) {
	request := &SendMessageRequestHeader{
		ProducerGroup:         "producerGroup",
		Topic:                 "topicValue",
		QueueId:               10,
		SysFlag:               10,
		BornTimestamp:         10,
		Flag:                  10,
		Properties:            "propertiesValue",
		ReconsumeTimes:        10,
		UnitMode:              false,
		MaxReconsumeTimes:     10,
		Batch:                 false,
		DefaultTopic:          "defaultTopicValue",
		DefaultTopicQueueNums: 10,
	}
	properties := map[string]string{
		"producerGroup":         "producerGroup",
		"topic":                 "topicValue",
		"queueId":               "10",
		"sysFlag":               "10",
		"bornTimestamp":         "10",
		"flag":                  "10",
		"reconsumeTimes":        "10",
		"unitMode":              "false",
		"maxReconsumeTimes":     "10",
		"defaultTopic":          "TBW102",
		"defaultTopicQueueNums": "4",
		"batch":                 "false",
		"properties":            "propertiesValue",
	}
	Convey("test SendMessageRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestSendMessageRequestV2Header_Encode(t *testing.T) {
	request := &SendMessageRequestV2Header{&SendMessageRequestHeader{
		ProducerGroup:         "producerGroup",
		Topic:                 "topicValue",
		QueueId:               10,
		SysFlag:               10,
		BornTimestamp:         10,
		Flag:                  10,
		Properties:            "propertiesValue",
		ReconsumeTimes:        10,
		UnitMode:              false,
		MaxReconsumeTimes:     10,
		Batch:                 false,
		DefaultTopic:          "defaultTopicValue",
		DefaultTopicQueueNums: 10,
	}}
	properties := map[string]string{
		"a": "producerGroup",
		"b": "topicValue",
		"c": "defaultTopicValue",
		"d": "10",
		"e": "10",
		"f": "10",
		"g": "10",
		"h": "10",
		"i": "propertiesValue",
		"j": "10",
		"k": "false",
		"l": "10",
		"m": "false",
	}
	Convey("test SendMessageRequestV2Header Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestTopicListRequestHeader_Encode(t *testing.T) {
	request := &TopicListRequestHeader{
		Topic: "topicValue",
	}
	properties := map[string]string{
		"topic": "topicValue",
	}
	Convey("test TopicListRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestUpdateConsumerOffsetRequestHeader_Encode(t *testing.T) {
	request := &UpdateConsumerOffsetRequestHeader{
		ConsumerGroup: "consumerGroupValue",
		Topic:         "topicValue",
		QueueId:       10,
		CommitOffset:  10,
	}
	properties := map[string]string{
		"consumerGroup": "consumerGroupValue",
		"topic":         "topicValue",
		"queueId":       "10",
		"commitOffset":  "10",
	}
	Convey("test UpdateConsumerOffsetRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}

func TestViewMessageRequestHeader_Encode(t *testing.T) {
	request := &ViewMessageRequestHeader{
		Offset: 10,
	}
	properties := map[string]string{
		"offset": "10",
	}
	Convey("test ViewMessageRequestHeader Encode method", t, func() {
		maps := request.Encode()
		So(maps, ShouldResemble, properties)
	})
}
