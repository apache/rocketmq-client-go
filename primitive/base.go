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

package primitive

import (
	"github.com/apache/rocketmq-client-go/v2/errors"
	"net"
	"net/url"
	"strings"
)

type NamesrvAddr []string

func NewNamesrvAddr(s ...string) (NamesrvAddr, error) {
	if len(s) == 0 {
		return nil, errors.ErrNoNameserver
	}

	ss := s
	if len(ss) == 1 {
		// compatible with multi server env string: "a;b;c"
		ss = strings.Split(s[0], ";")
	}

	for _, srv := range ss {
		if err := verifyAddr(srv); err != nil {
			return nil, err
		}
	}

	addrs := make(NamesrvAddr, 0)
	addrs = append(addrs, ss...)
	return addrs, nil
}

func (addr NamesrvAddr) Check() error {
	for _, srv := range addr {
		if err := verifyAddr(srv); err != nil {
			return err
		}
	}
	return nil
}

func verifyAddr(addr string) error {
	// url
	_, err := url.ParseRequestURI(addr)
	if err == nil {
		return nil
	}
	// ipv4, ipv6
	ip := net.ParseIP(addr)
	if ip != nil {
		return nil
	}
	// domain
	if isDomainName(addr) {
		return nil
	}
	return errors.ErrIllegalAddr
}

var PanicHandler func(interface{})

func WithRecover(fn func()) {
	defer func() {
		handler := PanicHandler
		if handler != nil {
			if err := recover(); err != nil {
				handler(err)
			}
		}
	}()

	fn()
}

func Diff(origin, latest []string) bool {
	if len(origin) != len(latest) {
		return true
	}

	// check added
	originFilter := make(map[string]struct{}, len(origin))
	for _, srv := range origin {
		originFilter[srv] = struct{}{}
	}

	latestFilter := make(map[string]struct{}, len(latest))
	for _, srv := range latest {
		if _, ok := originFilter[srv]; !ok {
			return true // added
		}
		latestFilter[srv] = struct{}{}
	}

	// check delete
	for _, srv := range origin {
		if _, ok := latestFilter[srv]; !ok {
			return true // deleted
		}
	}
	return false
}

// copy from go src/net/dnsclient.go
// https://github.com/golang/go/blob/master/src/net/dnsclient.go#L75

// isDomainName checks if a string is a presentation-format domain name
// (currently restricted to hostname-compatible "preferred name" LDH labels and
// SRV-like "underscore labels"; see golang.org/issue/12421).
func isDomainName(s string) bool {
	// The root domain name is valid. See golang.org/issue/45715.
	if s == "." {
		return true
	}

	// See RFC 1035, RFC 3696.
	// Presentation format has dots before every label except the first, and the
	// terminal empty label is optional here because we assume fully-qualified
	// (absolute) input. We must therefore reserve space for the first and last
	// labels' length octets in wire format, where they are necessary and the
	// maximum total length is 255.
	// So our _effective_ maximum is 253, but 254 is not rejected if the last
	// character is a dot.
	l := len(s)
	if l == 0 || l > 254 || l == 254 && s[l-1] != '.' {
		return false
	}

	last := byte('.')
	nonNumeric := false // true once we've seen a letter or hyphen
	partlen := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		default:
			return false
		case 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || c == '_':
			nonNumeric = true
			partlen++
		case '0' <= c && c <= '9':
			// fine
			partlen++
		case c == '-':
			// Byte before dash cannot be dot.
			if last == '.' {
				return false
			}
			partlen++
			nonNumeric = true
		case c == '.':
			// Byte before dot cannot be dot, dash.
			if last == '.' || last == '-' {
				return false
			}
			if partlen > 63 || partlen == 0 {
				return false
			}
			partlen = 0
		}
		last = c
	}
	if last == '-' || partlen > 63 {
		return false
	}

	return nonNumeric
}
