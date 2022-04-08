package rocketmq

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"google.golang.org/grpc"
)

func ClientLoggerInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	rlog.Info(fmt.Sprintf("gRPC request. method: %+v, request:%+v", method, req), nil)
	err := invoker(ctx, method, req, reply, cc, opts...)
	rlog.Info(fmt.Sprintf("gRPC response. method: %+v, response:%+v", method, reply), nil)
	return err
}
