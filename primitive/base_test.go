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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVerifyIP(t *testing.T) {
	IPs := "127.0.0.1:9876"
	err := verifyIP(IPs)
	assert.Nil(t, err)

	IPs = "12.24.123.243:10911"
	err = verifyIP(IPs)
	assert.Nil(t, err)

	IPs = "xa2.0.0.1:9876"
	err = verifyIP(IPs)
	assert.Equal(t, "IP addr error", err.Error())

	IPs = "333.0.0.1:9876"
	err = verifyIP(IPs)
	assert.Equal(t, "IP addr error", err.Error())

	IPs = "127.0.0.1:9876;12.24.123.243:10911"
	err = verifyIP(IPs)
	assert.Equal(t, "multiple IP addr does not support", err.Error())

	IPs = "bdbd:bdbd:ff:1:1:2:3:4:8888"
	err = verifyIP(IPs)
	assert.Nil(t, err)

	IPs = "[bdbd:bdbd:ff:1:1:2:3:4]:8888"
	err = verifyIP(IPs)
	assert.Nil(t, err)

	IPs = "[bdbd:bdbd:ff:1:1:2:3:4]:8888;[bdbd:bdbd:ff:1:1:2:3:4]:8889"
	err = verifyIP(IPs)
	assert.Equal(t, "multiple IP addr does not support", err.Error())
}

func TestBase(t *testing.T) {
	a := []string{}
	b := []string{}
	assert.False(t, Diff(a, b))

	a = []string{"a"}
	b = []string{"a", "b"}
	assert.True(t, Diff(a, b))

	a = []string{"a", "b", "c"}
	b = []string{"c", "a", "b"}
	assert.False(t, Diff(a, b))

	a = []string{"b", "a"}
	b = []string{"a", "c"}
	assert.True(t, Diff(a, b))
}
