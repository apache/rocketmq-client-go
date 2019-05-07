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

package rlog

import "github.com/sirupsen/logrus"

type Logger interface {
	Debug(i ...interface{})
	Debugf(format string, args ...interface{})
	Info(i ...interface{})
	Infof(format string, args ...interface{})
	Warn(i ...interface{})
	Warnf(format string, args ...interface{})
	Error(i ...interface{})
	Errorf(format string, args ...interface{})
	Fatal(i ...interface{})
	Fatalf(format string, args ...interface{})
}

var rLog Logger

func init() {
	r := logrus.New()
	//r.SetLevel(logrus.DebugLevel)
	rLog = r
}

func SetLogger(log Logger) {
	rLog = log
}

func Debug(i ...interface{}) {
	rLog.Debug(i...)
}

func Debugf(format string, args ...interface{}) {
	rLog.Debugf(format, args...)
}

func Info(i ...interface{}) {
	rLog.Info(i...)
}

func Infof(format string, args ...interface{}) {
	rLog.Infof(format, args...)
}

func Warn(i ...interface{}) {
	rLog.Warn(i...)
}

func Warnf(format string, args ...interface{}) {
	rLog.Warnf(format, args...)
}

func Error(i ...interface{}) {
	rLog.Error(i...)
}

func Errorf(format string, args ...interface{}) {
	rLog.Errorf(format, args...)
}

func Fatal(i ...interface{}) {
	rLog.Fatal(i...)
}

func Fatalf(format string, args ...interface{}) {
	rLog.Fatalf(format, args...)
}
