package admin

import (
	"encoding/json"
	"testing"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/stretchr/testify/assert"
)

func TestMap(t *testing.T) {

	MQTable := map[string]internal.ProcessQueueInfo{
		"hahah": {
			Locked: true,
		},
	}
	data, err := json.Marshal(MQTable)
	assert.Nil(t, err)
	t.Log("data info: ", string(data))

	b := map[string]internal.ProcessQueueInfo{}
	err = json.Unmarshal(data, &b)
	assert.Nil(t, err)
	t.Log("b: ", b)
}

func TestOffset(t *testing.T) {

	MQTable := map[consumer.MessageQueueKey]internal.ProcessQueueInfo{
		{
			Topic:      "a",
			BrokerName: "B-a",
			QueueId:    1,
		}: {
			Locked: true,
		},
	}
	data, err := json.Marshal(MQTable)
	assert.Nil(t, err)
	t.Log("data info: ", string(data))

	b := map[consumer.MessageQueueKey]internal.ProcessQueueInfo{}
	err = json.Unmarshal(data, &b)
	assert.Nil(t, err)
	t.Log("b: ", b)
}
