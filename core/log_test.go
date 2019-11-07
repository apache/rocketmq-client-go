/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package rocketmq

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLogConfig_String(t *testing.T) {
	logc := LogConfig{Path: "/log/path1", FileNum: 3, FileSize: 1 << 20, Level: LogLevelDebug}
	assert.Equal(t, "{Path:/log/path1 FileNum:3 FileSize:1048576 Level:Debug}", logc.String())
	logc.Level = LogLevelFatal
	assert.Equal(t, "{Path:/log/path1 FileNum:3 FileSize:1048576 Level:Fatal}", logc.String())
	logc.Level = LogLevelError
	assert.Equal(t, "{Path:/log/path1 FileNum:3 FileSize:1048576 Level:Error}", logc.String())
	logc.Level = LogLevelWarn
	assert.Equal(t, "{Path:/log/path1 FileNum:3 FileSize:1048576 Level:Warn}", logc.String())
	logc.Level = LogLevelInfo
	assert.Equal(t, "{Path:/log/path1 FileNum:3 FileSize:1048576 Level:Info}", logc.String())
	logc.Level = LogLevelTrace
	assert.Equal(t, "{Path:/log/path1 FileNum:3 FileSize:1048576 Level:Trace}", logc.String())
	logc.Level = LogLevelError
}
func TestLogLevel_String(t *testing.T) {
	logc := LogConfig{Path: "/log/path1", FileNum: 3, FileSize: 1 << 20, Level: LogLevelDebug}
	assert.Equal(t, "Debug", logc.Level.String())
}
