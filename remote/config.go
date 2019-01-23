package remote

import "time"

// client common config
type ClientConfig struct {
	nameServerAddress string // only this is in use

	clientIP     string
	instanceName string

	// Pulling topic information interval from the named server, default is 30
	pullNameServerInterval time.Duration

	// Heartbeat interval in microseconds with message broker, default is 30
	heartbeatBrokerInterval time.Duration

	// Offset persistent interval for consumer, default is 5
	persistConsumerOffsetInterval time.Duration

	unitMode          bool
	unitName          string
	vipChannelEnabled bool
}
