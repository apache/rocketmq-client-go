package rocketmq

import (
	v1 "github.com/apache/rocketmq-client-go/v2/Protos/apache/rocketmq/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"time"
)

type IClientManager interface {
	getRpcClient(target string) IRpcClient

	resolveRoute(target string, metadata metadata.MD, request v1.QueryRouteRequest, timeout time.Duration) (*TopicRouteData, error)

	heartbeat(target string, metadata metadata.MD, request v1.HeartbeatRequest, timeout time.Duration) (bool, error)

	notifyClientTermination(target string, metadata metadata.MD, request v1.NotifyClientTerminationRequest, timeout time.Duration) (bool, error)

	sendMessage(target string, metadata metadata.MD, request v1.SendMessageRequest, timeout time.Duration) (*v1.SendMessageResponse, error)
}

var _ IClientManager = new(ClientManage)

type ClientManage struct {
	rpcClients map[string]IRpcClient
}

func NewClientManage() *ClientManage {
	cm := &ClientManage{
		rpcClients: make(map[string]IRpcClient),
	}
	return cm
}

func (c *ClientManage) getRpcClient(target string) IRpcClient {
	cm, ok := c.rpcClients[target]
	if !ok {
		cm = NewRpcClient(target)
		c.rpcClients[target] = cm
	}
	cm, ok = c.rpcClients[target]
	if !ok {
		return nil
	}
	return c.rpcClients[target]
}

func (c *ClientManage) resolveRoute(target string, metadata metadata.MD, request v1.QueryRouteRequest, timeout time.Duration) (*TopicRouteData, error) {
	rpcClient := c.getRpcClient(target)
	queryRouteResponse, err := rpcClient.queryRoute(request, timeout, grpc.Header(&metadata))
	if queryRouteResponse == nil || err != nil || codes.Code(queryRouteResponse.Common.Status.Code) != codes.OK {
		//TODO Raise an application layer exception
		return nil, err
	}
	partitions := make([]*Partition, 0, len(queryRouteResponse.Partitions))
	for _, v := range queryRouteResponse.Partitions {
		if v.GetTopic() == nil {
			continue
		}
		topic := &Topic{
			name:              v.GetTopic().GetName(),
			resourceNamespace: v.GetTopic().GetResourceNamespace(),
		}
		if v.GetBroker() == nil {
			continue
		}
		broker := &Broker{
			name: v.GetBroker().GetName(),
			id:   v.GetBroker().GetId(),
		}
		if v.GetBroker().GetEndpoints() == nil {
			continue
		}
		endpoints := &Endpoints{
			scheme: v.GetBroker().GetEndpoints().GetScheme(),
		}
		broker.endpoints = endpoints
		if len(v.GetBroker().GetEndpoints().GetAddresses()) == 0 {
			continue
		}
		address := make([]*Address, 0, len(v.GetBroker().GetEndpoints().GetAddresses()))
		for _, addr := range v.GetBroker().GetEndpoints().GetAddresses() {
			if addr != nil {
				address = append(address, &Address{
					host: addr.GetHost(),
					port: addr.GetPort(),
				})
			}
		}
		endpoints.address = address
		if len(address) == 0 {
			continue
		}
		partition := &Partition{
			topic:      topic,
			broker:     broker,
			id:         v.GetId(),
			permission: v.GetPermission(),
		}
		partitions = append(partitions, partition)
	}
	return &TopicRouteData{
		partitions,
	}, nil
}

func (c *ClientManage) heartbeat(target string, metadata metadata.MD, request v1.HeartbeatRequest, timeout time.Duration) (bool, error) {
	rpcClient := c.getRpcClient(target)
	heartbeatResponse, err := rpcClient.heartbeat(request, timeout, grpc.Header(&metadata))
	if heartbeatResponse == nil || err != nil {
		return false, err
	}
	return codes.Code(heartbeatResponse.Common.Status.Code) == codes.OK, nil
}

func (c *ClientManage) notifyClientTermination(target string, metadata metadata.MD, request v1.NotifyClientTerminationRequest, timeout time.Duration) (bool, error) {
	rpcClient := c.getRpcClient(target)
	notifyClientTerminationResponse, err := rpcClient.notifyClientTermination(request, timeout, grpc.Header(&metadata))
	if notifyClientTerminationResponse == nil || err != nil {
		return false, err
	}
	return codes.Code(notifyClientTerminationResponse.Common.Status.Code) == codes.OK, nil
}

func (c *ClientManage) sendMessage(target string, metadata metadata.MD, request v1.SendMessageRequest, timeout time.Duration) (*v1.SendMessageResponse, error) {
	rpcClient := c.getRpcClient(target)
	return rpcClient.sendMessage(request, timeout, grpc.Header(&metadata))
}
