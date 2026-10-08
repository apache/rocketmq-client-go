/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package internal

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

// Trace clients have no registered consumers/producers. Their lifetime and
// discovery work belong to this pool, not the general MQ client registry.
// In particular, trace topics must be refreshed even without a producerMap entry.
type traceClientKey struct {
	shared      bool
	key, unit   string
	access      primitive.AccessChannel
	credentials primitive.Credentials
}

type traceClientEntry struct {
	ready  chan struct{}
	client *traceClient
	refs   int
}

var traceClients = struct {
	sync.Mutex
	entries map[traceClientKey]*traceClientEntry
}{entries: make(map[traceClientKey]*traceClientEntry)}

type traceClient struct {
	key      traceClientKey
	cli      *rmqClient
	namesrvs *namesrvs
	cleanup  func()
	cancel   context.CancelFunc
	done     chan struct{}
	topics   sync.Map
}

func isNilTraceValue(value interface{}) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// IsNilTraceDispatcher also handles a typed nil returned by a constructor.
func IsNilTraceDispatcher(dispatcher TraceDispatcher) bool {
	return isNilTraceValue(dispatcher)
}

func acquireTraceClient(cfg *primitive.TraceConfig, shared *primitive.SharedTraceClientConfig) (*traceClient, error) {
	if cfg == nil {
		return nil, fmt.Errorf("trace config is nil")
	}
	key := traceClientKey{unit: cfg.UnitName, access: cfg.Access, credentials: cfg.Credentials}
	var resolver primitive.NsResolver
	if shared != nil {
		if shared.Key == "" || shared.ResolverFactory == nil {
			return nil, fmt.Errorf("shared trace client requires a key and resolver factory")
		}
		key.shared, key.key = true, shared.Key
	} else {
		resolver = cfg.Resolver
		if len(cfg.NamesrvAddrs) > 0 {
			resolver = primitive.NewPassthroughResolver(append([]string(nil), cfg.NamesrvAddrs...))
		}
		if isNilTraceValue(resolver) {
			return nil, fmt.Errorf("no trace NamesrvAddrs or Resolver configured")
		}
	}

	for {
		traceClients.Lock()
		entry := traceClients.entries[key]
		if entry != nil {
			if entry.ready != nil {
				ready := entry.ready
				traceClients.Unlock()
				<-ready
				continue
			}
			entry.refs++
			client := entry.client
			traceClients.Unlock()
			// Preserve the legacy guard. Opt-in sharing uses logical identity,
			// so a discovery update cannot be mistaken for a different cluster.
			if shared == nil && !sameTraceAddresses(resolver.Resolve(), client.namesrvs.resolver.Resolve()) {
				client.release()
				return nil, fmt.Errorf("different namesrv option in the same trace instance")
			}
			return client, nil
		}
		entry = &traceClientEntry{ready: make(chan struct{}), refs: 1}
		traceClients.entries[key] = entry
		traceClients.Unlock()

		// User discovery code must not run under the pool lock.
		client, err := createTraceClient(key, cfg, shared, resolver)
		traceClients.Lock()
		if err != nil {
			delete(traceClients.entries, key)
		} else {
			entry.client = client
		}
		close(entry.ready)
		entry.ready = nil
		traceClients.Unlock()
		return client, err
	}
}

func sameTraceAddresses(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	a, b = append([]string(nil), a...), append([]string(nil), b...)
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func createTraceClient(key traceClientKey, cfg *primitive.TraceConfig, shared *primitive.SharedTraceClientConfig, resolver primitive.NsResolver) (*traceClient, error) {
	var cleanup func()
	var err error
	if shared != nil {
		resolver, cleanup, err = shared.ResolverFactory()
	}
	success := false
	defer func() {
		if !success && cleanup != nil {
			cleanup()
		}
	}()
	if err != nil {
		return nil, err
	}
	if isNilTraceValue(resolver) {
		return nil, fmt.Errorf("trace resolver factory returned nil")
	}
	srvs, err := NewNamesrv(resolver, nil)
	if err != nil {
		return nil, err
	}
	transport := remote.NewRemotingClient(nil)
	if !cfg.Credentials.IsEmpty() {
		srvs.SetCredentials(cfg.Credentials)
		transport.RegisterInterceptor(remote.ACLInterceptor(cfg.Credentials))
	}
	ctx, cancel := context.WithCancel(context.Background())
	options := DefaultClientOptions()
	options.Namesrv = srvs
	options.GroupName = cfg.GroupName
	options.UnitName = cfg.UnitName
	options.Credentials = cfg.Credentials
	options.InstanceName = "INNER_TRACE_CLIENT_DEFAULT"
	client := &traceClient{
		key: key, namesrvs: srvs, cleanup: cleanup, cancel: cancel, done: make(chan struct{}),
		// Only remoting and response decoding are used. Starting the general
		// MQ workers here would duplicate discovery and run empty heartbeats.
		cli: &rmqClient{option: options, remoteClient: transport},
	}
	go primitive.WithRecover(func() { client.refresh(ctx) })
	success = true
	return client, nil
}

func (client *traceClient) refresh(ctx context.Context) {
	client.runRefresh(ctx, 10*time.Second, 2*time.Minute, _PullNameServerInterval)
}

func (client *traceClient) runRefresh(ctx context.Context, initialNamesDelay, namesInterval, routesInterval time.Duration) {
	ctx, cancel := context.WithCancel(ctx)
	namesDone := make(chan struct{})
	go primitive.WithRecover(func() {
		defer close(namesDone)
		names := time.NewTimer(initialNamesDelay)
		defer names.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-names.C:
				client.namesrvs.UpdateNameServerAddress()
				names.Reset(namesInterval)
			}
		}
	})
	// Wait for both loops before the owner disposes the resolver or transports.
	defer func() {
		cancel()
		<-namesDone
		close(client.done)
	}()
	routes := time.NewTicker(routesInterval)
	defer routes.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-routes.C:
			client.refreshRoutes(ctx)
		}
	}
}

func (client *traceClient) refreshRoutes(ctx context.Context) {
	client.topics.Range(func(key, _ interface{}) bool {
		if ctx.Err() != nil {
			return false
		}
		// Preserve the per-NameServer timeout and fallback attempts. The worker
		// context cancels outstanding discovery when the resource is released.
		client.namesrvs.updateTopicRouteInfoWithContext(ctx, key.(string), "", 0)
		return true
	})
}

func (client *traceClient) release() {
	traceClients.Lock()
	entry := traceClients.entries[client.key]
	entry.refs--
	if entry.refs != 0 {
		traceClients.Unlock()
		return
	}
	// Keep a closing entry until cleanup completes. A replacement cannot reuse
	// a closed resolver or race with disposal of the previous connection pools.
	entry.ready = make(chan struct{})
	traceClients.Unlock()
	client.cancel()
	<-client.done
	client.cli.remoteClient.ShutDown()
	client.namesrvs.nameSrvClient.ShutDown()
	if client.cleanup != nil {
		client.cleanup()
	}
	traceClients.Lock()
	delete(traceClients.entries, client.key)
	close(entry.ready)
	traceClients.Unlock()
}
