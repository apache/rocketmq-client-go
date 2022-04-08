package rocketmq

import v1 "github.com/apache/rocketmq-client-go/v2/Protos/apache/rocketmq/v1"

type Endpoints struct {
	scheme  v1.AddressScheme
	address []*Address
}

func (e Endpoints) GetScheme() v1.AddressScheme {
	return e.scheme
}

func (e Endpoints) GetAddresses() []*Address {
	return e.address
}
