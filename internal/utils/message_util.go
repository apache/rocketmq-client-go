package utils

import (
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func createReplyMessage(requestMessage *primitive.Message, body []byte) *primitive.Message {
	if requestMessage != nil {
		var replayMessage *primitive.Message

		cluster := requestMessage.GetProperty("CLUSTER")
		replyTo := requestMessage.GetProperty("REPLY_TO_CLIENT")
		correlationId := requestMessage.GetProperty("CORRELATION_ID")
		ttl := requestMessage.GetProperty("TTL")
		replayMessage.UnmarshalProperties(body)

		if cluster != "" {
			var ms *primitive.Message
			replyTopic := ms.GetProperty(cluster)
			replayMessage.WithProperty("replyTopic", replyTopic)
			ms.WithProperty("MSG_TYPE", "reply")
			ms.WithProperty("CORRELATION_ID", correlationId)
			ms.WithProperty("REPLY_TO_CLIENT", replyTo)
			ms.WithProperty("TTL", ttl)

			return replayMessage

		}
		fmt.Printf("reply to %s , %s \n", replyTo, "create reply message fail, requestMessage error, property["+"CLUSTER"+"] is null.")
	}
	fmt.Printf("reply to %s , %s \n", "create reply message fail, requestMessage cannot be null.")
	return nil
}
func getReplyToClient(msg *primitive.Message) string {

	return msg.GetProperty("REPLY_TO_CLIENT")
}
