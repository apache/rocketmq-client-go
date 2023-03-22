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
	"fmt"
	"github.com/bilinxing/rocketmq-client-go/v2/rlog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/bilinxing/rocketmq-client-go/v2/primitive"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

// TestSelector test roundrobin selector in namesrv
func TestSelector(t *testing.T) {
	srvs := []string{"127.0.0.1:9876", "127.0.0.1:9879", "12.24.123.243:10911", "12.24.123.243:10915"}
	namesrv, err := NewNamesrv(primitive.NewPassthroughResolver(srvs), nil)
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

func TestUpdateNameServerAddress(t *testing.T) {
	Convey("Test UpdateNameServerAddress method", t, func() {
		srvs := []string{
			"192.168.100.1",
			"192.168.100.2",
			"192.168.100.3",
			"192.168.100.4",
			"192.168.100.5",
		}
		http.HandleFunc("/nameserver/addrs", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, strings.Join(srvs, ";"))
		})
		server := &http.Server{Addr: ":0", Handler: nil}
		listener, _ := net.Listen("tcp", ":0")
		go server.Serve(listener)

		port := listener.Addr().(*net.TCPAddr).Port
		nameServerDommain := fmt.Sprintf("http://127.0.0.1:%d/nameserver/addrs", port)
		rlog.Info("Temporary Nameserver", map[string]interface{}{
			"domain": nameServerDommain,
		})

		resolver := primitive.NewHttpResolver("DEFAULT", nameServerDommain)
		ns := &namesrvs{
			srvs:     []string{},
			lock:     new(sync.Mutex),
			resolver: resolver,
		}

		ns.UpdateNameServerAddress()

		index1 := ns.index
		IP1 := ns.getNameServerAddress()

		index2 := ns.index
		IP2 := ns.getNameServerAddress()

		So(index1+1, ShouldEqual, index2)
		So(IP1, ShouldEqual, srvs[index1])
		So(IP2, ShouldEqual, srvs[index2])
	})
}

func TestUpdateNameServerAddressUseEnv(t *testing.T) {
	Convey("Test UpdateNameServerAddress Use Env", t, func() {
		srvs := []string{
			"192.168.100.1",
			"192.168.100.2",
			"192.168.100.3",
			"192.168.100.4",
			"192.168.100.5",
		}

		resolver := primitive.NewEnvResolver()
		ns := &namesrvs{
			srvs:     []string{},
			lock:     new(sync.Mutex),
			resolver: resolver,
		}
		os.Setenv("NAMESRV_ADDR", strings.Join(srvs, ";"))
		ns.UpdateNameServerAddress()

		index1 := ns.index
		IP1 := ns.getNameServerAddress()

		index2 := ns.index
		IP2 := ns.getNameServerAddress()

		So(index1+1, ShouldEqual, index2)
		So(IP1, ShouldEqual, srvs[index1])
		So(IP2, ShouldEqual, srvs[index2])
	})
}
