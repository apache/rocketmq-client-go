## How to use

### go mod
```
require (
    github.com/bilinxing/rocketmq-client-go/v2 v2.1.0-rc3
)
```

### Set Logger
Go Client define the `Logger` interface for log output, user can specify implementation of private.
in default, client use `logrus`.
```
rlog.SetLogger(Logger)
```

### Send message
#### Interface
```
Producer interface {
	Start() error
	Shutdown() error
	SendSync(context.Context, *primitive.Message) (*internal.SendResult, error)
	SendOneWay(context.Context, *primitive.Message) error
}
```

#### Examples
- create a new `Producer` instance
```
p, err := rocketmq.NewProducer(
		producer.WithNameServer(endPoint),
		//producer.WithNsResolver(primitive.NewPassthroughResolver(endPoint)),
		producer.WithRetry(2),
		producer.WithGroupName("GID_xxxxxx"),
	)
```

- start the producer
```go 
err := p.Start()
```

- send message with sync
```
result, err := p.SendSync(context.Background(), &primitive.Message{
    Topic: "test",
    Body:  []byte("Hello RocketMQ Go Client!"),
})

// do something with result
```

- or send message with oneway
```
err := p.SendOneWay(context.Background(), &primitive.Message{
    Topic: "test",
    Body:  []byte("Hello RocketMQ Go Client!"),
})
```
Full examples: [producer](../examples/producer)

### Consume Message
now only support `PushConsumer`

#### Interface
```
PushConsumer interface {
	// Start the PullConsumer for consuming message
	Start() error

	// Shutdown the PullConsumer, all offset of MessageQueue will be sync to broker before process exit
	Shutdown() error
	// Subscribe a topic for consuming
	Subscribe(topic string, selector consumer.MessageSelector,
		f func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) error
}
```

#### Usage
- Create a `PushConsumer` instance
```
c, err := rocketmq.NewPushConsumer(
		consumer.WithNameServer(endPoint),
        consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithGroupName("GID_XXXXXX"),
	)
```

- Subscribe a topic(only support one topic now), and define your consuming function
```
err := c.Subscribe("test", consumer.MessageSelector{}, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
    rlog.Info("Subscribe Callback", map[string]interface{}{
        "msgs": msgs,
    })
    return consumer.ConsumeSuccess, nil
})
```
- start the consumer(**NOTE: MUST after subscribe**)
```
err = c.Start()
```

Full examples: [consumer](../examples/consumer)


### Admin: Topic Operation

#### Examples
- create topic
```
testAdmin, err := admin.NewAdmin(admin.WithResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})))
err = testAdmin.CreateTopic(
	context.Background(),
	admin.WithTopicCreate("newTopic"),
	admin.WithBrokerAddrCreate("127.0.0.1:10911"),
)
```

- delete topic
`ClusterName` not supported yet
```
err = testAdmin.DeleteTopic(
	context.Background(),
	admin.WithTopicDelete("newTopic"),
	//admin.WithBrokerAddrDelete("127.0.0.1:10911"),	//optional
	//admin.WithNameSrvAddr(nameSrvAddr),				//optional
)
```