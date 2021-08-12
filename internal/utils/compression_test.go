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
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
)

func TestUnCompress(t *testing.T) {
	var b bytes.Buffer
	var oriStr string = "hello, go"
	zr := zlib.NewWriter(&b)
	zr.Write([]byte(oriStr))
	zr.Close()

	retBytes := UnCompress(b.Bytes())
	if string(retBytes) != oriStr {
		t.Errorf("UnCompress was incorrect, got %s, want: %s .", retBytes, []byte(oriStr))
	}
}

func TestCompress(t *testing.T) {
	raw := []byte("The quick brown fox jumps over the lazy dog")
	for i := zlib.BestSpeed; i <= zlib.BestCompression; i++ {
		compressed, e := Compress(raw, i)
		if e != nil {
			t.Errorf("Compress data:%s returns error: %v", string(raw), e)
			return
		}
		decompressed := UnCompress(compressed)
		if string(decompressed) != string(raw) {
			t.Errorf("data is corrupt, got: %s, want: %s", string(decompressed), string(raw))
		}
	}
}

func testCase(data []byte, level int, t *testing.T) {
	compressed, e := Compress(data, level)
	if e != nil {
		t.Errorf("Compress data:%v returns error: %v", data, e)
	}
	decompressed := UnCompress(compressed)
	if string(data) != string(decompressed) {
		t.Errorf("data is corrupt, got: %s, want: %s", string(decompressed), string(data))
	}
}

func generateRandTestData(n int) []byte {
	data := make([]byte, n)
	rand.Read(data)
	return data
}

func generateJsonString(n int) []byte {
	x := make(map[string]string)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("compression_key_%d", i)
		v := fmt.Sprintf("compression_value_%d", i)
		x[k] = v
	}
	data, _ := json.Marshal(x)
	return data
}

func TestCompressThreadSafe(t *testing.T) {
	for i := 0; i < 100; i++ {
		data := generateRandTestData(i * 100)
		level := i%zlib.BestCompression + 1
		go testCase(data, level, t)
	}

	for i := 0; i < 100; i++ {
		data := generateJsonString(i * 100)
		level := i%zlib.BestCompression + 1
		go testCase(data, level, t)
	}
}
