package producer

import (
	"github.com/apache/rocketmq-client-go/internal/kernel"
	"github.com/apache/rocketmq-client-go/primitive"
)

func defaultProducerOptions() producerOptions {
	return producerOptions{
		ClientOptions: *kernel.DefaultClientOptions(),
	}
}

type producerOptions struct {
	kernel.ClientOptions
}

type Option func(*producerOptions)

// WithGroupName set group name address
func WithGroupName(group string) Option {
	return func(opts *producerOptions) {
		if group == "" {
			return
		}
		opts.GroupName = group
	}
}

// WithNameServer set NameServer address, only support one NameServer cluster in alpha2
func WithNameServer(nameServers ...[]string) Option {
	return func(opts *producerOptions) {
		if len(nameServers) > 0 {
			opts.NameServerAddrs = nameServers[0]
		}
	}
}

// WithACL on/off ACL
func WithVIPChannel(enable bool) Option {
	return func(opts *producerOptions) {
		opts.VIPChannelEnabled = enable
	}
}

// WithACL on/off ACL
func WithACL(enable bool) Option {
	return func(opts *producerOptions) {
		opts.ACLEnabled = enable
	}
}

// WithRetry return a Option that specifies the retry times when send failed.
// TODO: use retry middleware instead
func WithRetry(retries int) Option {
	return func(opts *producerOptions) {
		opts.RetryTimes = retries
	}
}

func WithInterceptor(f ...primitive.Interceptor) Option {
	return func(opts *producerOptions) {
		opts.Interceptors = append(opts.Interceptors, f...)
	}
}
