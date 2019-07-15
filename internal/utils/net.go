package utils

import (
	"errors"
	"fmt"
	"net"
)

func LocalIP() string {
	ip, err := clientIP4()
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%d.%d.%d.%d", ip[0], ip[1], ip[2], ip[3])
}

func clientIP4() ([]byte, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, errors.New("unexpected IP address")
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ip4 := ipnet.IP.To4(); ip4 != nil {
				return ip4, nil
			}
		}
	}
	return nil, errors.New("unknown IP address")
}
