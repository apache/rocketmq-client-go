package internal

import (
	"errors"
	"fmt"

	"github.com/apache/rocketmq-client-go/v2/primitive"
)

// CreateReplyMessage build reply message from the request message
func CreateReplyMessage(requestMessage *primitive.MessageExt, body []byte) (*primitive.Message, error) {
	if requestMessage == nil {
		return nil, errors.New("create reply message fail, requestMessage cannot be null")
	}

	cluster := requestMessage.GetProperty(primitive.PropertyCluster)
	replyTo := requestMessage.GetProperty(primitive.PropertyMessageReplyToClient)
	correlationId := requestMessage.GetProperty(primitive.PropertyCorrelationID)
	ttl := requestMessage.GetProperty(primitive.PropertyMessageTTL)

	if cluster == "" {
		return nil, fmt.Errorf("create reply message fail, requestMessage error, property[\"%s\"] is null", cluster)
	}

	var replayMessage primitive.Message

	replayMessage.UnmarshalProperties(body)
	replayMessage.Topic = GetReplyTopic(cluster)
	replayMessage.WithProperty(primitive.PropertyMsgType, ReplyMessageFlag)
	replayMessage.WithProperty(primitive.PropertyCorrelationID, correlationId)
	replayMessage.WithProperty(primitive.PropertyMessageReplyToClient, replyTo)
	replayMessage.WithProperty(primitive.PropertyMessageTTL, ttl)

	return &replayMessage, nil
}

func GetReplyToClient(msg *primitive.MessageExt) string {
	return msg.GetProperty(primitive.PropertyMessageReplyToClient)
}
