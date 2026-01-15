package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func main() {

	nameSrvAddr := []string{"127.0.0.1:9876"}

	testAdmin, err := admin.NewAdmin(
		admin.WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)),
		admin.WithCredentials(primitive.Credentials{
			AccessKey: "RocketMQ",
			SecretKey: "12345678",
		}),
	)

	// cluster info
	result, err := testAdmin.GetBrokerClusterInfo(context.Background())
	if err != nil {
		fmt.Println("GetBrokerClusterInfo error:", err.Error())
	}
	fmt.Println(result)

	err = testAdmin.Close()
	if err != nil {
		fmt.Printf("Shutdown admin error: %s", err.Error())
	}
}
