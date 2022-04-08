package rocketmq

import (
	"fmt"
	"os"
	"time"
)

type IClientConfig interface {
	Region() string

	ServiceName() string

	ResourceNamespace() string

	CredentialsProvider() ICredentialsProvider

	TenantId() string

	GetIoTimeout() time.Duration

	GetLongPollingTimeout() time.Duration

	GetGroupName() string

	ClientId() string

	IsTracingEnabled() bool
}

type ClientConfig struct {
	region_ string

	serviceName_ string

	resourceNamespace_ string

	credentialsProvider_ ICredentialsProvider

	tenantId_ string

	ioTimeout_ time.Time

	longPollingIoTimeout_ time.Time

	groupName_ string

	clientId_ string

	tracingEnabled_ bool

	instanceName_ string
}

func (c *ClientConfig) Region() string {
	return c.region_
}

func (c *ClientConfig) SetRegion(region string) {
	c.region_ = region
}

func (c *ClientConfig) ServiceName() string {
	return c.serviceName_
}

func (c *ClientConfig) SetServiceName(serviceName string) {
	c.serviceName_ = serviceName
}

func (c *ClientConfig) ResourceNamespace() string {
	return c.resourceNamespace_
}

func (c *ClientConfig) SetResourceNamespace(resourceNamespace string) {
	c.resourceNamespace_ = resourceNamespace
}

func (c *ClientConfig) CredentialsProvider() ICredentialsProvider {
	return c.credentialsProvider_
}

func (c *ClientConfig) SetCredentialsProvider(credentialsProvider ICredentialsProvider) {
	c.credentialsProvider_ = credentialsProvider
}

func (c *ClientConfig) TenantId() string {
	return c.tenantId_
}

func (c *ClientConfig) SetTenantId(tenantId_ string) {
	c.tenantId_ = tenantId_
}

func (c *ClientConfig) GetIoTimeout() time.Time {
	return c.ioTimeout_
}

func (c *ClientConfig) SetIoTimeout(ioTimeout time.Time) {
	c.ioTimeout_ = ioTimeout
}

func (c *ClientConfig) GetLongPollingTimeout() time.Time {
	return c.longPollingIoTimeout_
}

func (c *ClientConfig) SetLongPollingTimeout(longPollingIoTimeout time.Time) {
	c.longPollingIoTimeout_ = longPollingIoTimeout
}

func (c *ClientConfig) GetGroupName() string {
	return c.groupName_
}

func (c *ClientConfig) SetGroupName(groupName string) {
	c.groupName_ = groupName
}

func (c *ClientConfig) ClientId() string {
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "default_host_name"
	}
	pid := os.Getpid()
	return fmt.Sprintf("%s@%d#%s", hostName, pid, c.instanceName_)
}

func (c *ClientConfig) IsTracingEnabled() bool {
	return c.tracingEnabled_
}

func (c *ClientConfig) SetTracingEnabled(tracingEnabled bool) {
	c.tracingEnabled_ = tracingEnabled
}

func NewClientConfig() *ClientConfig {
	c := &ClientConfig{
		region_:         "cn-hangzhou",
		serviceName_:    "ONS",
		tracingEnabled_: false,
		instanceName_:   "default",
	}
	return c
}
