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
	"strings"
)

type NamesrvAddr []string

func NewNamesrvAddr(s ...string) (NamesrvAddr, error) {
	if len(s) == 0 {
		return nil, ErrNoNameserver
	}

	ss := s
	if len(ss) == 1 {
		// compatible with multi server env string: "a;b;c"
		ss = strings.Split(s[0], ";")
	}

	addrs := make(NamesrvAddr, 0)
	addrs = append(addrs, ss...)
	return addrs, nil
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
