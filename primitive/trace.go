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

// config for message trace.
type TraceConfig struct {
	TraceTopic   string
	GroupName    string
	UnitName     string
	Access       AccessChannel
	NamesrvAddrs []string
	Resolver     NsResolver
	Credentials  // acl config for trace. omit if acl is closed on broker.
}

// SharedTraceClientConfig identifies a trace transport independently of the
// current NameServer addresses. Use a stable key for the logical cluster (for
// example its discovery endpoint and tenant), never a consumer group or an IP
// list. UnitName, Access and Credentials from TraceConfig also partition clients.
type SharedTraceClientConfig struct {
	Key string
	// ResolverFactory is called once per shared client's lifetime. It must return
	// an independently owned resolver, not a consumer's resolver. The optional
	// cleanup function is called once after the last dispatcher closes (also on
	// initialization failure). Resolve must return promptly and be safe for
	// concurrent use. NamesrvAddrs and Resolver in TraceConfig are ignored.
	ResolverFactory func() (NsResolver, func(), error)
}
