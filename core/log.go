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

/*
#include <rocketmq/CCommon.h>
*/
import "C"
import "fmt"

// LogLevel the log level
type LogLevel int

// predefined log level
const (
	LogLevelFatal = LogLevel(C.E_LOG_LEVEL_FATAL)
	LogLevelError = LogLevel(C.E_LOG_LEVEL_ERROR)
	LogLevelWarn  = LogLevel(C.E_LOG_LEVEL_WARN)
	LogLevelInfo  = LogLevel(C.E_LOG_LEVEL_INFO)
	LogLevelDebug = LogLevel(C.E_LOG_LEVEL_DEBUG)
	LogLevelTrace = LogLevel(C.E_LOG_LEVEL_TRACE)
	LogLevelNum   = LogLevel(C.E_LOG_LEVEL_LEVEL_NUM)
)

func (l LogLevel) String() string {
	switch l {
	case LogLevelFatal:
		return "Fatal"
	case LogLevelError:
		return "Error"
	case LogLevelWarn:
		return "Warn"
	case LogLevelInfo:
		return "Info"
	case LogLevelDebug:
		return "Debug"
	case LogLevelTrace:
		return "Trace"
	case LogLevelNum:
		return "Num"
	default:
		return "Unkonw"
	}
}

// LogConfig the log configuration for the pull consumer
type LogConfig struct {
	Path     string
	FileNum  int
	FileSize int64
	Level    LogLevel
}

func (lc *LogConfig) String() string {
	return fmt.Sprintf("%+v", *lc)
}
