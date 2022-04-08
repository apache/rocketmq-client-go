package rocketmq

import (
	v1 "github.com/apache/rocketmq-client-go/v2/Protos/apache/rocketmq/v1"
	"strconv"
	"strings"
)

func IsNullOrEmpty(s string) bool {
	return len(s) == 0
}

func String2V1Address(s string) *v1.Address {
	pos := strings.LastIndex(s, ":")
	host := s[:pos]
	port, err := strconv.Atoi(s[pos+1:])
	if err != nil {
		// TODO log error
		return nil
	}
	address := &v1.Address{
		Host: host,
		Port: int32(port),
	}
	return address
}

func CompareNameServers(n1 []string, n2 []string) bool {
	// TODO CompareNameServers
	if n1 == nil || n2 == nil {
		return false
	}
	if len(n1) != len(n2) {
		return false
	}
	return true
}

func ComparePartitions(p1 []*Partition, p2 []*Partition) bool {
	// TODO ComparePartitions
	if p1 == nil || p2 == nil {
		return false
	}
	if len(p1) != len(p2) {
		return false
	}
	return true
}

func CompareTopicRouteDatas(t1 *TopicRouteData, t2 *TopicRouteData) bool {
	// TODO CompareTopicRouteDatas
	if t1 == t2 {
		return true
	}
	if t1 == nil || t2 == nil {
		return false
	}
	return ComparePartitions(t1.Partitions(), t2.Partitions())
}

func CompareAddress(a1 *v1.Address, a2 *v1.Address) bool {
	if a1 == a2 {
		return true
	}
	if a1 == nil || a2 == nil {
		return false
	}
	if a1.GetHost() != a2.GetHost() {
		return false
	}
	if a1.GetPort() != a2.GetPort() {
		return false
	}
	return true
}

func CompareAddressList(a1 []*Address, a2 []*Address) bool {
	//TODO CompareAddresses
	if a1 == nil && a2 == nil {
		return true
	}
	if len(a1) != len(a2) {
		return false
	}
	return true
}

func CompareEndpoints(e1 *Endpoints, e2 *Endpoints) bool {
	if e1 == e2 {
		return true
	}
	if e1 == nil || e2 == nil {
		return false
	}
	if e1.GetScheme() != e2.GetScheme() {
		return false
	}
	return CompareAddressList(e1.GetAddresses(), e2.GetAddresses())
}

func CompareBroker(b1 *Broker, b2 *Broker) bool {
	if b1 == b2 {
		return true
	}
	if b1 == nil || b2 == nil {
		return false
	}
	if b1.GetId() != b2.GetId() {
		return false
	}
	if b1.GetName() != b2.GetName() {
		return false
	}
	return CompareEndpoints(b1.GetEndpoints(), b2.GetEndpoints())
}
