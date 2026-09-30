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

	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/stretchr/testify/require"
)

func ownershipTestOptions(t *testing.T) ClientOptions {
	options := DefaultClientOptions()
	options.InstanceName = t.Name()
	ns, err := NewNamesrv(primitive.NewPassthroughResolver([]string{"127.0.0.2:9876", "127.0.0.1:9876"}), nil)
	require.NoError(t, err)
	options.Namesrv = ns
	return options
}

func TestClientOwnershipConcurrentAcquireAndRelease(t *testing.T) {
	const workers = 16
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				client := GetOrNewRocketMQClient(ownershipTestOptions(t), nil)
				if client == nil {
					t.Error("acquisition failed")
					return
				}
				actual := client.(*rmqClient)
				if actual.GetNameSrv().(*namesrvs).bundleClient != actual {
					t.Error("client published before initialization")
				}
				select {
				case <-actual.done:
					t.Error("acquired a closed client")
				default:
				}
				client.Shutdown()
			}
		}()
	}
	wg.Wait()
	options := ownershipTestOptions(t)
	_, exists := clientMap.Load((&rmqClient{option: options}).ClientID())
	require.False(t, exists, "all acquisitions must be released")
}

func TestClientOwnershipOldCleanupKeepsReplacement(t *testing.T) {
	options := ownershipTestOptions(t)
	old := GetOrNewRocketMQClient(options, nil).(*rmqClient)
	old.Shutdown()
	replacement := GetOrNewRocketMQClient(options, nil).(*rmqClient)
	defer replacement.Shutdown()
	require.NotSame(t, old, replacement)
	old.Shutdown()
	stored, exists := clientMap.Load(replacement.ClientID())
	require.True(t, exists)
	require.Same(t, replacement, stored)
}

func TestClientOwnershipKeepsResolverSnapshot(t *testing.T) {
	options := ownershipTestOptions(t)
	addresses := []string{"127.0.0.2:9876", "127.0.0.1:9876"}
	ns, err := NewNamesrv(primitive.NewPassthroughResolver(addresses), nil)
	require.NoError(t, err)
	options.Namesrv = ns
	first := GetOrNewRocketMQClient(options, nil)
	require.NotNil(t, first)
	defer first.Shutdown()
	second := GetOrNewRocketMQClient(options, nil)
	require.NotNil(t, second)
	defer second.Shutdown()
	require.Equal(t, []string{"127.0.0.2:9876", "127.0.0.1:9876"}, addresses,
		"compatibility checks must not sort a resolver-owned snapshot in place")
}
