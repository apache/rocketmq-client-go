package rocketmq

import "fmt"

type Broker struct {
	name      string
	id        int32
	endpoints *Endpoints
}

func (b Broker) targetUrl() string {
	var addr = b.endpoints.address[0]
	return fmt.Sprintf("https://%s:%d", addr.host, addr.port)
}

func (b Broker) GetId() int32 {
	return b.id
}

func (b Broker) GetName() string {
	return b.name
}

func (b Broker) GetEndpoints() *Endpoints {
	return b.endpoints
}
