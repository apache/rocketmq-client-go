package rocketmq

import (
	"context"
	v1 "github.com/apache/rocketmq-client-go/v2/Protos/apache/rocketmq/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"time"
)

type IRpcClient interface {
	queryRoute(request v1.QueryRouteRequest, timeout time.Duration, callOptions ...grpc.CallOption) (*v1.QueryRouteResponse, error)

	heartbeat(request v1.HeartbeatRequest, timeout time.Duration, callOptions ...grpc.CallOption) (*v1.HeartbeatResponse, error)

	notifyClientTermination(request v1.NotifyClientTerminationRequest, timeout time.Duration, callOptions ...grpc.CallOption) (*v1.NotifyClientTerminationResponse, error)

	sendMessage(request v1.SendMessageRequest, timeout time.Duration, callOptions ...grpc.CallOption) (*v1.SendMessageResponse, error)
}

var _ IRpcClient = new(RpcClient)

type RpcClient struct {
	stub v1.MessagingServiceClient
}

func NewRpcClient(target string) *RpcClient {
	cc, err := grpc.Dial(target, grpc.WithInsecure(), grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:    60 * time.Second,
		Timeout: 30 * time.Second,
	}), grpc.WithUnaryInterceptor(ClientLoggerInterceptor))
	if err != nil {
		//TODO log create rpcClient error
	}
	cli := &RpcClient{
		stub: v1.NewMessagingServiceClient(cc),
	}
	return cli
}

func (r RpcClient) queryRoute(request v1.QueryRouteRequest, timeout time.Duration, callOptions ...grpc.CallOption) (*v1.QueryRouteResponse, error) {
	deadline := time.Now().Add(timeout)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	route, err := r.stub.QueryRoute(ctx, &request, callOptions...)
	if err != nil {
		return nil, err
	}
	return route, nil
}

func (r RpcClient) heartbeat(request v1.HeartbeatRequest, timeout time.Duration, callOptions ...grpc.CallOption) (*v1.HeartbeatResponse, error) {
	deadline := time.Now().Add(timeout)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	response, err := r.stub.Heartbeat(ctx, &request, callOptions...)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (r RpcClient) notifyClientTermination(request v1.NotifyClientTerminationRequest, timeout time.Duration, callOptions ...grpc.CallOption) (*v1.NotifyClientTerminationResponse, error) {
	deadline := time.Now().Add(timeout)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	response, err := r.stub.NotifyClientTermination(ctx, &request, callOptions...)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (r RpcClient) sendMessage(request v1.SendMessageRequest, timeout time.Duration, callOptions ...grpc.CallOption) (*v1.SendMessageResponse, error) {
	deadline := time.Now().Add(timeout)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	response, err := r.stub.SendMessage(ctx, &request, callOptions...)
	if err != nil {
		return nil, err
	}
	return response, nil
}
