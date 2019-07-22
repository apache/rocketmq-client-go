package utils

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"
)

var (
	LocalIP string
)

func init() {
	ip, err := ClientIP4()
	if err != nil {
		LocalIP = ""
	} else {
		LocalIP = fmt.Sprintf("%d.%d.%d.%d", ip[0], ip[1], ip[2], ip[3])
	}
}

func ClientIP4() ([]byte, error) {
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

func FakeIP() []byte {
	buf := bytes.NewBufferString("")
	buf.WriteString(strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10))
	return buf.Bytes()[4:8]
}
