package primitive

import (
	"time"
)

type RemotingClientConfig struct {
	TcpOption
}

type TcpOption struct {
	KeepAliveDuration time.Duration
	ConnectionTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
}
