package rocketmq

import (
	"fmt"
	v1 "github.com/apache/rocketmq-client-go/v2/Protos/apache/rocketmq/v1"
	"google.golang.org/grpc/codes"
	metadata "google.golang.org/grpc/metadata"
)

type IProducer interface {
	Start()
	Shutdown()
	Send(message Message) (SendResult, error)
}

var _ IProducer = new(Producer)

type Producer struct {
	client *RMQClientImpl
	IProducer

	loadBalancer map[string]PublishLoadBalancer
}

func NewProducer(resolver INameServerResolver, resourceNamespace string) (*Producer, error) {
	p := &Producer{
		client: NewClient(resolver, resourceNamespace),
	}
	return p, nil
}

func (p *Producer) Start() {
	fmt.Println("producer start")
	p.client.Start()
}

func (p *Producer) Shutdown() {
	fmt.Println("producer shutdown")
	p.client.Shutdown()
}

func (p *Producer) Send(message Message) (SendResult, error) {
	_, ok := p.loadBalancer[message.Topic]
	if !ok {
		topicRouteData, err := p.client.getRouteFor(message.Topic, false)
		if topicRouteData == nil || err != nil || topicRouteData.Partitions() == nil {
			//TODO throw error
			return SendResult{
				Status: SendUnknownError,
			}, err
		}
		p.loadBalancer[message.Topic] = NewPublishLoadBalancer(*topicRouteData)
	}
	publishLB := p.loadBalancer[message.Topic]
	request := v1.SendMessageRequest{}
	request.Message = &v1.Message{}
	request.Message.Body = make([]byte, len(message.Body))
	copy(request.Message.Body, message.Body)
	request.Message.Topic = &v1.Resource{}
	request.Message.Topic.ResourceNamespace = p.client.ResourceNamespace()
	request.Message.Topic.Name = message.Topic
	for k, v := range message.UserProperties {
		request.Message.UserAttribute[k] = v
	}
	request.Message.SystemAttribute = &v1.SystemAttribute{}
	request.Message.SystemAttribute.MessageId = message.MessageId
	if !IsNullOrEmpty(message.Tag) {
		request.Message.SystemAttribute.Tag = message.Tag
	}
	if len(message.Keys) != 0 {
		for _, v := range message.Keys {
			request.Message.SystemAttribute.Keys = append(request.Message.SystemAttribute.Keys, v)
		}
	}
	// string target = "https://";
	var targets []string

	candidates := publishLB.selectPartitions(message.MaxAttemptTimes)
	for _, partition := range candidates {
		targets = append(targets, partition.GetBroker().targetUrl())
	}

	metaData := metadata.MD{}
	sign(p.client, metaData)

	var err error
	for _, target := range targets {
		var response *v1.SendMessageResponse
		response, err = p.client.clientManager.sendMessage(target, metaData, request, p.client.GetIoTimeout())
		if response != nil && codes.Code(response.Common.Status.Code) == codes.OK {
			return SendResult{
				Status: SendOK,
				MsgID:  response.GetMessageId(),
			}, nil
		}
	}

	return SendResult{
		Status: SendUnknownError,
	}, err
}
