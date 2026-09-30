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

package rocketmq_test

import (
	"testing"

	rocketmq "github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/stretchr/testify/require"
)

type clientOwner interface{ Shutdown() error }

func TestClientOwnershipBeforeStart(t *testing.T) {
	constructors := map[string]func(string, string) (clientOwner, error){
		"producer": func(instance, address string) (clientOwner, error) {
			return rocketmq.NewProducer(producer.WithInstanceName(instance), producer.WithNameServer([]string{address}))
		},
		"push": func(instance, address string) (clientOwner, error) {
			return rocketmq.NewPushConsumer(consumer.WithInstance(instance), consumer.WithNameServer([]string{address}))
		},
		"pull": func(instance, address string) (clientOwner, error) {
			return rocketmq.NewPullConsumer(consumer.WithInstance(instance), consumer.WithNameServer([]string{address}))
		},
	}
	for name, create := range constructors {
		t.Run(name, func(t *testing.T) {
			instance := t.Name()
			clientID := internal.DefaultClientOptions().ClientIP + "@" + instance
			first, err := create(instance, "127.0.0.1:9876")
			require.NoError(t, err)
			defer first.Shutdown()
			second, err := create(instance, "127.0.0.1:9876")
			require.NoError(t, err)
			defer second.Shutdown()
			require.NoError(t, first.Shutdown())
			require.NoError(t, first.Shutdown())
			_, err = internal.GetNamesrv(clientID)
			require.NoError(t, err, "another unstarted owner still needs the shared client")
			_, err = create(instance, "127.0.0.2:9876")
			require.Error(t, err, "a live owner's NameServer compatibility check must remain in force")
			require.NoError(t, second.Shutdown())
			_, err = internal.GetNamesrv(clientID)
			require.Error(t, err, "the final owner must release the registry entry, including after a rejected acquisition")
			replacement, err := create(instance, "127.0.0.2:9876")
			require.NoError(t, err, "the same instance must be reusable with new addresses")
			require.NoError(t, replacement.Shutdown())
		})
	}
}

func TestClientOwnershipProtectsRunningProducer(t *testing.T) {
	instance := t.Name()
	clientID := internal.DefaultClientOptions().ClientIP + "@" + instance
	create := func(group string) (rocketmq.Producer, error) {
		return rocketmq.NewProducer(producer.WithInstanceName(instance), producer.WithGroupName(group), producer.WithNameServer([]string{"127.0.0.1:9876"}))
	}
	active, err := create("active-group")
	require.NoError(t, err)
	defer active.Shutdown()
	require.NoError(t, active.Start())
	require.NoError(t, active.Start())
	for i := 0; i < 2; i++ {
		duplicate, err := create("active-group")
		require.NoError(t, err)
		require.Error(t, duplicate.Start(), "failed owners must not unregister the active producer")
		require.NoError(t, duplicate.Shutdown())
	}
	pending, err := create("pending-group")
	require.NoError(t, err)
	defer pending.Shutdown()
	require.NoError(t, active.Shutdown())
	_, err = internal.GetNamesrv(clientID)
	require.NoError(t, err, "the unstarted owner must retain its client after the active owner exits")
	require.NoError(t, pending.Start())
	require.NoError(t, pending.Shutdown())
	_, err = internal.GetNamesrv(clientID)
	require.Error(t, err)
}
