package rocketmq

import (
	"fmt"
	"testing"
)

func TestDefaultConsumer_Pull(t *testing.T) {
	consumer := NewConsumer(ConsumerConfig{
		GroupName: "testGroup",
	})
	consumer.Start()
	result, err := consumer.Pull("test", "*", 32)
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println(len(result.GetMessageExts()))
}
