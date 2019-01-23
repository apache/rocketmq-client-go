package remote

import "time"

// client common config
type ClientConfig struct {
	nameServerAddress string // only this is in use

	clientIP                      string
	instanceName                  string
	clientCallbackExecutorThreads int // TODO: clientCallbackExecutorThreads
	// Pulling topic information interval from the named server
	pullNameServerInterval time.Duration // default 30
	// Heartbeat interval in microseconds with message broker
	heartbeatBrokerInterval time.Duration // default 30
	// Offset persistent interval for consumer
	persistConsumerOffsetInterval time.Duration // default 5
	unitMode                      bool
	unitName                      string
	vipChannelEnabled             bool
}
