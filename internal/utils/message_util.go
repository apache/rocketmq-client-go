package utils

import (

	"github.com/apache/rocketmq-client-go/v2/primitive"
)


func createReplyMessage(requestMessage *primitive.Message,body []byte) (*primitive.Message,) {
	if requestMessage != nil {
		var replayMessage  *primitive.Message

		cluster:=	requestMessage.GetProperty("CLUSTER")
		replyTo:=	requestMessage.GetProperty("REPLY_TO_CLIENT")
		correlationId:=    requestMessage.GetProperty("CORRELATION_ID")
		ttl       :=   requestMessage.GetProperty("TTL")
		replayMessage.UnmarshalProperties(body)

			)
		if cluster != nil {
		var ms	*primitive.Message
		replyTopic :=ms.GetProperty(cluster)




		}
	}

	replyMessage := 
	return replyMessage
}
func getReplyToClient(msg *primitive.Message)string{

	return  msg.GetProperty("REPLY_TO_CLIENT")
}

