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
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/apache/rocketmq-client-go/v2/rlog"

	. "github.com/smartystreets/goconvey/convey"
)

func TestEnvResolver(t *testing.T) {
	Convey("Test UpdateNameServerAddress Use Env", t, func() {
		srvs := []string{
			"192.168.100.1",
			"192.168.100.2",
			"192.168.100.3",
			"192.168.100.4",
			"192.168.100.5",
		}

		resolver := NewEnvResolver()
		os.Setenv("NAMESRV_ADDR", strings.Join(srvs, ";"))

		addrs := resolver.Resolve()

		So(Diff(srvs, addrs), ShouldBeFalse)
	})
}

func TestHttpResolverWithGet(t *testing.T) {
	Convey("Test UpdateNameServerAddress Save Local Snapshot", t, func() {
		srvs := []string{
			"192.168.100.1",
			"192.168.100.2",
			"192.168.100.3",
			"192.168.100.4",
			"192.168.100.5",
		}
		http.HandleFunc("/nameserver/addrs2", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, strings.Join(srvs, ";"))
		})
		server := &http.Server{Addr: ":0", Handler: nil}
		listener, _ := net.Listen("tcp", ":0")
		go server.Serve(listener)

		port := listener.Addr().(*net.TCPAddr).Port
		nameServerDommain := fmt.Sprintf("http://127.0.0.1:%d/nameserver/addrs2", port)
		rlog.Info("Temporary Nameserver", map[string]interface{}{
			"domain": nameServerDommain,
		})

		resolver := NewHttpResolver("DEFAULT", nameServerDommain)
		resolver.Resolve()

		// check snapshot saved
		filePath := resolver.getSnapshotFilePath("DEFAULT")
		body := strings.Join(srvs, ";")
		bs, _ := ioutil.ReadFile(filePath)
		So(string(bs), ShouldEqual, body)
	})
}

func TestHttpResolverWithGetUnitName(t *testing.T) {
	Convey("Test UpdateNameServerAddress Save Local Snapshot", t, func() {
		srvs := []string{
			"192.168.100.1",
			"192.168.100.2",
			"192.168.100.3",
			"192.168.100.4",
			"192.168.100.5",
		}
		http.HandleFunc("/nameserver/addrs3-unsh", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("nofix") == "1" {
				fmt.Fprintf(w, strings.Join(srvs, ";"))
			}
			fmt.Fprintf(w, "")
		})
		server := &http.Server{Addr: ":0", Handler: nil}
		listener, _ := net.Listen("tcp", ":0")
		go server.Serve(listener)

		port := listener.Addr().(*net.TCPAddr).Port
		nameServerDommain := fmt.Sprintf("http://127.0.0.1:%d/nameserver/addrs3", port)
		rlog.Info("Temporary Nameserver", map[string]interface{}{
			"domain": nameServerDommain,
		})

		resolver := NewHttpResolver("DEFAULT", nameServerDommain)
		resolver.DomainWithUnit("unsh")
		resolver.Resolve()

		// check snapshot saved
		filePath := resolver.getSnapshotFilePath("DEFAULT")
		body := strings.Join(srvs, ";")
		bs, _ := ioutil.ReadFile(filePath)
		So(string(bs), ShouldEqual, body)
	})
}

func TestHttpResolverWithSnapshotFile(t *testing.T) {
	Convey("Test UpdateNameServerAddress Use Local Snapshot", t, func() {
		srvs := []string{
			"192.168.100.1",
			"192.168.100.2",
			"192.168.100.3",
			"192.168.100.4",
			"192.168.100.5",
		}

		resolver := NewHttpResolver("DEFAULT", "http://127.0.0.1:80/error/nsaddrs")

		os.Setenv("NAMESRV_ADDR", "") // clear env
		// setup local snapshot file
		filePath := resolver.getSnapshotFilePath("DEFAULT")
		body := strings.Join(srvs, ";")
		_ = ioutil.WriteFile(filePath, []byte(body), 0644)

		addrs := resolver.Resolve()

		So(Diff(addrs, srvs), ShouldBeFalse)
	})
}

func TesHttpReslverWithSnapshotFileOnce(t *testing.T) {
	Convey("Test UpdateNameServerAddress Load Local Snapshot Once", t, func() {
		srvs := []string{
			"192.168.100.1",
			"192.168.100.2",
			"192.168.100.3",
			"192.168.100.4",
			"192.168.100.5",
		}

		resolver := NewHttpResolver("DEFAULT", "http://127.0.0.1:80/error/nsaddrs")

		os.Setenv("NAMESRV_ADDR", "") // clear env
		// setup local snapshot file
		filePath := resolver.getSnapshotFilePath("DEFAULT")
		body := strings.Join(srvs, ";")
		_ = ioutil.WriteFile(filePath, []byte(body), 0644)
		// load local snapshot file first time
		addrs1 := resolver.Resolve()

		// change the local snapshot file to check load once
		_ = ioutil.WriteFile(filePath, []byte("127.0.0.1;127.0.0.2"), 0644)

		addrs2 := resolver.Resolve()

		So(Diff(addrs1, addrs2), ShouldBeFalse)
		So(Diff(addrs1, srvs), ShouldBeFalse)
	})
}
