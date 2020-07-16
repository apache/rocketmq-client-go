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
	"os"
	"strings"

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
	Level(level string)
	OutputPath(path string) (err error)
}

func init() {
	r := &defaultLogger{
		logger: logrus.New(),
	}
	level := os.Getenv("ROCKETMQ_GO_LOG_LEVEL")
	switch strings.ToLower(level) {
	case "debug":
		r.logger.SetLevel(logrus.DebugLevel)
	case "warn":
		r.logger.SetLevel(logrus.WarnLevel)
	case "error":
		r.logger.SetLevel(logrus.ErrorLevel)
	default:
		r.logger.SetLevel(logrus.InfoLevel)
	}
	rLog = r
}

var rLog Logger

type defaultLogger struct {
	logger *logrus.Logger
}

func (l *defaultLogger) Debug(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Debug(msg)
}

func (l *defaultLogger) Info(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Info(msg)
}

func (l *defaultLogger) Warning(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Warning(msg)
}

func (l *defaultLogger) Error(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).WithFields(fields).Error(msg)
}

func (l *defaultLogger) Fatal(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Fatal(msg)
}

func (l *defaultLogger) Level(level string) {
	switch strings.ToLower(level) {
	case "debug":
		l.logger.SetLevel(logrus.DebugLevel)
	case "warn":
		l.logger.SetLevel(logrus.WarnLevel)
	case "error":
		l.logger.SetLevel(logrus.ErrorLevel)
	default:
		l.logger.SetLevel(logrus.InfoLevel)
	}
}

func (l *defaultLogger) OutputPath(path string) (err error) {
	var file *os.File
	file, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return
	}

	l.logger.Out = file
	return
}

// SetLogger use specified logger user customized, in general, we suggest user to replace the default logger with specified
func SetLogger(logger Logger) {
	rLog = logger
}
func SetLogLevel(level string) {
	if level == "" {
		return
	}
	rLog.Level(level)
}

func SetOutputPath(path string) (err error) {
	if "" == path {
		return
	}

	return rLog.OutputPath(path)
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
