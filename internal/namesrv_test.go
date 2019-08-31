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
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

// TestSelector test roundrobin selector in namesrv
func TestSelector(t *testing.T) {
	srvs := []string{"127.0.0.1:9876", "127.0.0.1:9879", "12.24.123.243:10911", "12.24.123.243:10915"}
	namesrv, err := NewNamesrv(srvs)
	assert.Nil(t, err)

	assert.Equal(t, srvs[0], namesrv.getNameServerAddress())
	assert.Equal(t, srvs[1], namesrv.getNameServerAddress())
	assert.Equal(t, srvs[2], namesrv.getNameServerAddress())
	assert.Equal(t, srvs[3], namesrv.getNameServerAddress())
	assert.Equal(t, srvs[0], namesrv.getNameServerAddress())
	assert.Equal(t, srvs[1], namesrv.getNameServerAddress())
	assert.Equal(t, srvs[2], namesrv.getNameServerAddress())
	assert.Equal(t, srvs[3], namesrv.getNameServerAddress())
	assert.Equal(t, srvs[0], namesrv.getNameServerAddress())
}

func TestGetNamesrv(t *testing.T) {
	Convey("Test GetNamesrv round-robin strategy", t, func() {
		ns := &namesrvs{
			srvs: []string{"192.168.100.1",
				"192.168.100.2",
				"192.168.100.3",
				"192.168.100.4",
				"192.168.100.5",
			},
			lock: new(sync.Mutex),
		}

		index1 := ns.index
		IP1 := ns.getNameServerAddress()

		index2 := ns.index
		IP2 := ns.getNameServerAddress()

		So(index1+1, ShouldEqual, index2)
		So(IP1, ShouldEqual, ns.srvs[index1])
		So(IP2, ShouldEqual, ns.srvs[index2])
	})
}
