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

package internal

import (
	"errors"
	"regexp"
	"strings"
	"sync"
)

var (
	ipRegex, _ = regexp.Compile(`^((25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))\.){3}(25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))`)

	ErrNoNameserver = errors.New("nameServerAddrs can't be empty.")
	ErrMultiIP      = errors.New("multiple IP addr does not support")
	ErrIllegalIP    = errors.New("IP addr error")
)

// Namesrvs rocketmq namesrv instance.
type Namesrvs struct {
	// namesrv addr list
	srvs []string

	// lock for getNamesrv in case of update index race condition
	lock sync.Locker

	// index indicate the next position for getNamesrv
	index int
}

// NewNamesrv init Namesrv from namesrv addr string.
func NewNamesrv(s ...string) (*Namesrvs, error) {
	if len(s) == 0 {
		return nil, ErrNoNameserver
	}

	ss := s
	if len(ss) == 1 {
		// compatible with multi server env string: "a;b;c"
		ss = strings.Split(s[0], ";")
	}

	for _, srv := range ss {
		if err := verifyIP(srv); err != nil {
			return nil, err
		}
	}

	return &Namesrvs{
		srvs: ss,
		lock: new(sync.Mutex),
	}, nil
}

// GetNamesrv return namesrv using round-robin strategy.
func (s *Namesrvs) GetNamesrv() string {
	s.lock.Lock()
	defer s.lock.Unlock()

	addr := s.srvs[s.index]
	index := s.index + 1
	if index < 0 {
		index = -index
	}
	index %= len(s.srvs)
	s.index = index
	return addr
}

func (s *Namesrvs) Size() int {
	return len(s.srvs)
}

func (s *Namesrvs) String() string {
	return strings.Join(s.srvs, ";")
}

func verifyIP(ip string) error {
	if strings.Contains(ip, ";") {
		return ErrMultiIP
	}
	ips := ipRegex.FindAllString(ip, -1)
	if len(ips) == 0 {
		return ErrIllegalIP
	}

	if len(ips) > 1 {
		return ErrMultiIP
	}
	return nil
}
