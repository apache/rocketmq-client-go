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

package consumer

import (
	"testing"
	"time"
)

func almostEqual(a, b float64) bool {
	diff := abs(a - b)
	return diff/a < 0.01
}

func abs(a float64) float64 {
	if a > 0 {
		return a
	}
	return -a
}

func TestNextMinuteTime(t *testing.T) {
	nextMinute := nextMinutesTime()
	minuteElapse := nextMinute.Sub(time.Now()).Minutes()
	if !almostEqual(minuteElapse, 1.0) {
		t.Errorf("wrong next one minute. want=%f, got=%f", 1.0, minuteElapse)
	}
}

func TestNextHourTime(t *testing.T) {
	nextHour := nextHourTime()
	hourElapse := nextHour.Sub(time.Now()).Hours()
	if !almostEqual(hourElapse, 1.0) {
		t.Errorf("wrong next one hour. want=%f, got=%f", 1.0, hourElapse)
	}
}

func TestIncreasePullRTGetPullRT(t *testing.T) {
	mgr := NewStatsManager()
	mgr.ShutDownStat()

	tests := []struct {
		RT        int64
		ExpectSum int64
	}{
		{1, 0},
		{1, 1},
		{1, 2},
		{1, 3},
		{1, 4},
		{1, 5},
		{1, 6},
		{1, 6},
	}
	for _, tt := range tests {
		mgr.increasePullRT("rocketmq", "default", tt.RT)
		mgr.topicAndGroupPullRT.samplingInSeconds()
		snapshot := mgr.getPullRT("rocketmq", "default")
		if snapshot.sum != tt.ExpectSum {
			t.Errorf("wrong Pull RT sum. want=%d, got=%d", tt.ExpectSum, snapshot.sum)
		}
	}
}

//func TestIncreaseConsumeRTGetConsumeRT(t *testing.T) {
//	ShutDownStat()
//	tests := []struct {
//		RT        int64
//		ExpectSum int64
//	}{
//		{1, 0},
//		{1, 1},
//		{1, 2},
//		{1, 3},
//		{1, 4},
//		{1, 5},
//		{1, 6},
//		{1, 6},
//	}
//	for _, tt := range tests {
//		increaseConsumeRT("rocketmq", "default", tt.RT)
//		topicAndGroupConsumeRT.samplingInMinutes()
//		snapshot := getConsumeRT("rocketmq", "default")
//		if snapshot.sum != tt.ExpectSum {
//			t.Errorf("wrong consume RT sum. want=%d, got=%d", tt.ExpectSum, snapshot.sum)
//		}
//	}
//}

func TestIncreasePullTPSGetPullTPS(t *testing.T) {
	mgr := NewStatsManager()
	mgr.ShutDownStat()
	tests := []struct {
		RT        int
		ExpectSum int64
	}{
		{1, 0},
		{1, 1},
		{1, 2},
		{1, 3},
		{1, 4},
		{1, 5},
		{1, 6},
		{1, 6},
	}
	for _, tt := range tests {
		mgr.increasePullTPS("rocketmq", "default", tt.RT)
		mgr.topicAndGroupPullTPS.samplingInSeconds()
		snapshot := mgr.getPullTPS("rocketmq", "default")
		if snapshot.sum != tt.ExpectSum {
			t.Errorf("wrong Pull TPS sum. want=%d, got=%d", tt.ExpectSum, snapshot.sum)
		}
	}
}

func TestIncreaseConsumeOKTPSGetConsumeOKTPS(t *testing.T) {
	mgr := NewStatsManager()
	mgr.ShutDownStat()
	tests := []struct {
		RT        int
		ExpectSum int64
	}{
		{1, 0},
		{1, 1},
		{1, 2},
		{1, 3},
		{1, 4},
		{1, 5},
		{1, 6},
		{1, 6},
	}
	for _, tt := range tests {
		mgr.increaseConsumeOKTPS("rocketmq", "default", tt.RT)
		mgr.topicAndGroupConsumeOKTPS.samplingInSeconds()
		snapshot := mgr.getConsumeOKTPS("rocketmq", "default")
		if snapshot.sum != tt.ExpectSum {
			t.Errorf("wrong Consume OK TPS sum. want=%d, got=%d", tt.ExpectSum, snapshot.sum)
		}
	}
}

func TestIncreaseConsumeFailedTPSGetConsumeFailedTPS(t *testing.T) {
	mgr := NewStatsManager()
	mgr.ShutDownStat()
	tests := []struct {
		RT        int
		ExpectSum int64
	}{
		{1, 0},
		{1, 1},
		{1, 2},
		{1, 3},
		{1, 4},
		{1, 5},
		{1, 6},
		{1, 6},
	}
	for _, tt := range tests {
		mgr.increaseConsumeFailedTPS("rocketmq", "default", tt.RT)
		mgr.topicAndGroupConsumeFailedTPS.samplingInSeconds()
		snapshot := mgr.getConsumeFailedTPS("rocketmq", "default")
		if snapshot.sum != tt.ExpectSum {
			t.Errorf("wrong Consume Failed TPS sum. want=%d, got=%d", tt.ExpectSum, snapshot.sum)
		}
	}
}

func TestGetConsumeStatus(t *testing.T) {
	mgr := NewStatsManager()
	mgr.ShutDownStat()
	group, topic := "rocketmq", "default"

	tests := []struct {
		RT                int
		ExpectFailMessage int64
	}{
		{1, 0},
		{1, 1},
		{1, 2},
		{1, 3},
		{1, 4},
	}
	for _, tt := range tests {
		mgr.increasePullRT(group, topic, int64(tt.RT))
		mgr.increasePullTPS(group, topic, tt.RT)
		mgr.increaseConsumeRT(group, topic, int64(tt.RT))
		mgr.increaseConsumeOKTPS(group, topic, tt.RT)
		mgr.increaseConsumeFailedTPS(group, topic, tt.RT)
		mgr.topicAndGroupPullRT.samplingInSeconds()
		mgr.topicAndGroupPullTPS.samplingInSeconds()
		mgr.topicAndGroupConsumeRT.samplingInMinutes()
		mgr.topicAndGroupConsumeOKTPS.samplingInSeconds()
		mgr.topicAndGroupConsumeFailedTPS.samplingInMinutes()
		status := mgr.GetConsumeStatus(group, topic)
		if status.ConsumeFailedMsgs != tt.ExpectFailMessage {
			t.Errorf("wrong ConsumeFailedMsg. want=0, got=%d", status.ConsumeFailedMsgs)
		}
	}
}

func TestNewStatsManager(t *testing.T) {
	stats := NewStatsManager()

	st := time.Now()
	for {
		stats.increasePullTPS("rocketmq", "default", 1)
		time.Sleep(500 * time.Millisecond)
		if time.Now().Sub(st) > 5*time.Minute {
			break
		}
	}
	stats.ShutDownStat()
}
