package main

import (
	"github.com/apache/rocketmq-client-go/v2"
	"time"
)

func main() {
	namesrvs := rocketmq.NewStaticStaticNameServerResolver([]string{"127.0.0.1:9876"})
	p, err := rocketmq.NewProducer(namesrvs, "defaultNamespace")
	if err != nil {
		panic(err)
	}
	p.Start()
	time.Sleep(30 * time.Second)
}
