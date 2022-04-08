package rocketmq

import (
	"fmt"
	v1 "github.com/apache/rocketmq-client-go/v2/Protos/apache/rocketmq/v1"
	"google.golang.org/grpc/metadata"
	"sync"
	"time"
)

type IClient interface {
	Heartbeat() error

	HealthCheck() error

	NotifyClientTermination() (bool, error)
}

type Client interface {
	IClient

	IClientConfig

	Start()
	Shutdown()
}

var _ Client = new(RMQClientImpl)

type RMQClient interface {
	Client

	prepareHeartbeatData(request v1.HeartbeatRequest)
}

type RMQClientImpl struct {
	RMQClient

	once         sync.Once
	shutdownOnce sync.Once
	done         chan struct{}

	clientManager      IClientManager
	nameServerResolver INameServerResolver
	//nameServerResolverCTS CancellationTokenSource
	nameServers            []string
	currentNameServerIndex int
	topicRouteTable        map[string]*TopicRouteData
	//updateTopicRouteCTS CancellationTokenSource

}

func NewClient(resolver INameServerResolver, resourceNamespace string) *RMQClientImpl {
	c := &RMQClientImpl{
		nameServerResolver: resolver,
		clientManager:      GetClientManager(resourceNamespace),
		done:               make(chan struct{}),
	}
	return c
}

func (c *RMQClientImpl) Start() {
	fmt.Println("client start")
	c.once.Do(func() {
		fmt.Println("do once")
		WithSchedulerAndRecover(func() {
			c.updateNameServerList()
		}, 10*time.Second, 30*time.Second, c.done, func() {
			fmt.Println("updateNameServerList over")
		})

		WithSchedulerAndRecover(func() {
			c.updateTopicRoute()
		}, 10*time.Second, 30*time.Second, c.done, func() {
			fmt.Println("updateTopicRoute over")
		})
	})
}

func (c *RMQClientImpl) Shutdown() {
	c.shutdownOnce.Do(func() {
		close(c.done)
	})
}

func (c *RMQClientImpl) updateNameServerList() error {
	fmt.Println("updateNameServerList")
	fmt.Println(c.nameServers)
	nameServers := c.nameServerResolver.ResolveAsync()
	if len(nameServers) == 0 {
		// something wrong
		return fmt.Errorf("nameServerLiar is empty")
	}
	if CompareNameServers(c.nameServers, nameServers) {
		return nil
	}
	// Name server list is updated.
	// TODO: Locking is required
	c.nameServers = nameServers
	c.currentNameServerIndex = 0
	return nil
}

func (c *RMQClientImpl) updateTopicRoute() error {
	fmt.Println("updateTopicRoute")
	if len(c.nameServers) == 0 {
		c.updateNameServerList()
		if len(c.nameServers) == 0 {
			// TODO: log warning here.
			return nil
		}
	}
	//nameServer := c.nameServers[c.currentNameServerIndex]
	var result []*TopicRouteData = make([]*TopicRouteData, 0, len(c.topicRouteTable))
	for key, _ := range c.topicRouteTable {
		topicRouteData, err := c.getRouteFor(key, true)
		if err != nil {
			return err
		}
		result = append(result, topicRouteData)
	}
	// Update topic route data
	for _, item := range result {
		if item == nil {
			continue
		}
		if len(item.Partitions()) == 0 {
			continue
		}
		topicName := item.Partitions()[0].GetTopic().GetName()
		prev, ok := c.topicRouteTable[topicName]
		if !ok || !CompareTopicRouteDatas(item, prev) {
			c.topicRouteTable[topicName] = item
		}
	}
	return nil
}

func (c *RMQClientImpl) getRouteFor(topic string, direct bool) (*TopicRouteData, error) {
	val, ok := c.topicRouteTable[topic]
	if !direct && ok {
		return val, nil
	}
	if len(c.nameServers) == 0 {
		err := c.updateNameServerList()
		if err != nil {
			return nil, err
		}
		if len(c.nameServers) == 0 {
			// TODO: log warning here.
			return nil, nil
		}
	}

	// We got one or more name servers available.
	nameServer := c.nameServers[c.currentNameServerIndex]
	request := v1.QueryRouteRequest{}
	request.Topic = &v1.Resource{
		ResourceNamespace: c.ResourceNamespace(),
		Name:              topic,
	}
	address := String2V1Address(nameServer)
	request.Endpoints = &v1.Endpoints{
		Scheme: v1.AddressScheme_IPv4,
	}
	request.Endpoints.Addresses = append(request.Endpoints.Addresses, address)
	fmt.Sprintf("")
	var target = fmt.Sprintf("https://%s:%d", address.Host, address.Port)
	var metaData = metadata.MD{}
	sign(c, metaData)

	return c.clientManager.resolveRoute(target, metaData, request, c.GetIoTimeout())
}

func (c *RMQClientImpl) Heartbeat() error {
	endpoints := c.endpointsInUse()
	if len(endpoints) == 0 {
		return nil
	}
	heartbeatRequest := v1.HeartbeatRequest{}
	c.prepareHeartbeatData(heartbeatRequest)

	metaData := metadata.MD{}
	sign(c, metaData)
	for _, endpoint := range endpoints {
		result, err := c.clientManager.heartbeat(endpoint, metaData, heartbeatRequest, c.GetIoTimeout())
		if !result || err != nil {
			return err
		}
	}
	return nil
}

func (c *RMQClientImpl) HealthCheck() error {
	return nil
}

func (c *RMQClientImpl) endpointsInUse() []string {
	//TODO: gather endpoints from route entries.
	var endpoints []string
	return endpoints
}

func (c *RMQClientImpl) NotifyClientTermination() (bool, error) {
	endpoints := c.endpointsInUse()
	request := v1.NotifyClientTerminationRequest{}
	request.ClientId = c.ClientId()

	metaData := metadata.MD{}
	sign(c, metaData)

	for _, endpoint := range endpoints {
		result, err := c.clientManager.notifyClientTermination(endpoint, metaData, request, c.GetIoTimeout())
		if !result || err != nil {
			return false, err
		}
	}
	return true, nil
}
