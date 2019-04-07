/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

var (
	counter        int16 = 0
	startTimestamp int64 = 0
	nextTimestamp  int64 = 0
	prefix         string
	locker         sync.Mutex
)

func MessageClientID() string {
	locker.Lock()
	defer locker.Unlock()
	if prefix == "" {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.BigEndian, LocalIP())
		binary.Write(buf, binary.BigEndian, Pid())
		binary.Write(buf, binary.BigEndian, ClassLoaderID())
		prefix = fmt.Sprintf("%x", buf.Bytes())
	}
	if time.Now().Unix() > nextTimestamp {
		updateTimestamp()
	}
	counter++
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, int32((time.Now().Unix()-startTimestamp)*1000))
	binary.Write(buf, binary.BigEndian, counter)
	return prefix + fmt.Sprintf("%x", buf.Bytes())

}

func updateTimestamp() {
	year, month := time.Now().Year(), time.Now().Month()
	startTimestamp = int64(time.Date(year, month, 1, 0, 0, 0, 0, time.Local).Unix())
	nextTimestamp = int64(time.Date(year, month, 1, 0, 0, 0, 0, time.Local).AddDate(0, 1, 0).Unix())
}

func LocalIP() []byte {
	ip, err := clientIP4()
	if err != nil {
		return []byte{0, 0, 0, 0}
	}
	return ip
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

func GetAddressByBytes(data []byte) string {
	return "127.0.0.1"
}

func Pid() int16 {
	return int16(os.Getpid())
}

func ClassLoaderID() int32 {
	return 0
}

func UnCompress(data []byte) []byte {
	return data
}

func IsArrayEmpty(i interface{}) bool {
	arr, ok := i.([]interface{})
	if !ok {
		return true
	}
	return arr == nil || len(arr) == 0
}