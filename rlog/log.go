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

import (
	"github.com/sirupsen/logrus"
)

const (
	LogKeyConsumerGroup    = "consumerGroup"
	LogKeyTopic            = "topic"
	LogKeyMessageQueue     = "MessageQueue"
	LogKeyUnderlayError    = "underlayError"
	LogKeyBroker           = "broker"
	LogKeyValueChangedFrom = "changedFrom"
	LogKeyValueChangedTo   = "changeTo"
	LogKeyPullRequest      = "PullRequest"
)

type Logger interface {
	Debug(msg string, fields map[string]interface{})
	Info(msg string, fields map[string]interface{})
	Warning(msg string, fields map[string]interface{})
	Error(msg string, fields map[string]interface{})
	Fatal(msg string, fields map[string]interface{})
}

func init() {
	r := &defaultLogger{
		logger: logrus.New(),
	}
	rLog = r
}

var rLog *defaultLogger

type defaultLogger struct {
	logger *logrus.Logger
}

func (l *defaultLogger) Debug(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	rLog.logger.WithFields(fields).Debug(msg)
}

func (l *defaultLogger) Info(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	rLog.logger.WithFields(fields).Info(msg)
}

func (l *defaultLogger) Warning(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	rLog.logger.WithFields(fields).Warning(msg)
}

func (l *defaultLogger) Error(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	rLog.logger.WithFields(fields).WithFields(fields).Error(msg)
}

func (l *defaultLogger) Fatal(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	rLog.logger.WithFields(fields).Fatal(msg)
}

func Debug(msg string, fields map[string]interface{}) {
	rLog.Debug(msg, fields)
}

func Info(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	rLog.Info(msg, fields)
}

func Warning(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	rLog.Warning(msg, fields)
}

func Error(msg string, fields map[string]interface{}) {
	rLog.Error(msg, fields)
}

func Fatal(msg string, fields map[string]interface{}) {
	rLog.Fatal(msg, fields)
}
