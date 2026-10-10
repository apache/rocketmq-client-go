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

package utils

import (
	"testing"
)

func TestLocalIP2(t *testing.T) {
	t.Log(LocalIP)
}

func TestAdaptIPv6(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "valid IPv6 address with port",
			input:    "2001:db8::1:8080",
			expected: "[2001:db8::1]:8080",
		},
		{
			name:     "valid IPv6 address with port - short format",
			input:    "::1:8080",
			expected: "[::1]:8080",
		},
		{
			name:     "valid IPv6 address with port - full format",
			input:    "2001:0db8:85a3:0000:0000:8a2e:0370:7334:8080",
			expected: "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:8080",
		},
		{
			name:     "already bracketed IPv6 address",
			input:    "[2001:db8::1]:8080",
			expected: "[2001:db8::1]:8080",
		},
		{
			name:     "already bracketed IPv6 address - short format",
			input:    "[::1]:8080",
			expected: "[::1]:8080",
		},
		{
			name:     "IPv4 address should return original",
			input:    "192.168.1.1:8080",
			expected: "192.168.1.1:8080",
		},
		{
			name:     "IPv4 address should return original - localhost",
			input:    "127.0.0.1:8080",
			expected: "127.0.0.1:8080",
		},
		{
			name:     "invalid address format - no port",
			input:    "2001:db8::1",
			expected: "2001:db8::1",
		},
		{
			name:     "invalid address format - invalid port",
			input:    "2001:db8::1:invalid",
			expected: "2001:db8::1:invalid",
		},
		{
			name:     "invalid address format - empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "invalid address format - port only",
			input:    ":8080",
			expected: ":8080",
		},
		{
			name:     "invalid address format - colon only",
			input:    ":",
			expected: ":",
		},
		{
			name:     "invalid address format - multiple colons but no valid IPv6",
			input:    "invalid:address:8080",
			expected: "invalid:address:8080",
		},
		{
			name:     "domain name should return original",
			input:    "localhost:8080",
			expected: "localhost:8080",
		},
		{
			name:     "domain name should return original - with dots",
			input:    "example.com:8080",
			expected: "example.com:8080",
		},
		{
			name:     "IPv6 address with large port number",
			input:    "2001:db8::1:65535",
			expected: "[2001:db8::1]:65535",
		},
		{
			name:     "IPv6 address with small port number",
			input:    "2001:db8::1:1",
			expected: "[2001:db8::1]:1",
		},
		{
			name:     "IPv6 address with zero port number",
			input:    "2001:db8::1:0",
			expected: "[2001:db8::1]:0",
		},
		{
			name:     "complex IPv6 address",
			input:    "fe80::1%lo0:8080",
			expected: "fe80::1%lo0:8080",
		},
		{
			name:     "IPv6 address with zone identifier",
			input:    "fe80::1%eth0:8080",
			expected: "fe80::1%eth0:8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AdaptIPv6(tt.input)
			if result != tt.expected {
				t.Errorf("AdaptIPv6(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}
