package kernel

import (
	"context"
	"testing"
)

func TestRMQClient_PullMessage(t *testing.T) {
	client := GetOrNewRocketMQClient(ClientOption{})
	req := &PullMessageRequest{
		ConsumerGroup:  "testGroup",
		Topic:          "wenfeng",
		QueueId:        0,
		QueueOffset:    0,
		MaxMsgNums:     32,
		SysFlag:        0x1 << 2,
		SubExpression:  "*",
		ExpressionType: "TAG",
	}
	res, err := client.PullMessage(context.Background(), "127.0.0.1:10911", req)
	if err != nil {
		t.Fatal(err.Error())
	}

	for _, a := range res.GetMessageExts() {
		t.Log(string(a.Body))
	}
}
