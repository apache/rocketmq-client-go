package latency

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFaultStrategy_UpdateFaultItem(t *testing.T) {

	var lft latencyFaultTolerance

	type fields struct {
		LatencyFaultHandler latencyFaultHandler
	}
	type args struct {
		brokerName     string
		currentLatency int64
		isolation      bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{name: "level1", fields: struct{ LatencyFaultHandler latencyFaultHandler }{&lft}, args: args{"broker-a", 20, true}},
		{name: "level2", fields: struct{ LatencyFaultHandler latencyFaultHandler }{&lft}, args: args{"broker-a", 90, true}},
		{name: "level3", fields: struct{ LatencyFaultHandler latencyFaultHandler }{&lft}, args: args{"broker-a", 500, true}},
		{name: "level4", fields: struct{ LatencyFaultHandler latencyFaultHandler }{&lft}, args: args{"broker-a", 800, false}},
		{name: "level5", fields: struct{ LatencyFaultHandler latencyFaultHandler }{&lft}, args: args{"broker-a", 1500, false}},
		{name: "level6", fields: struct{ LatencyFaultHandler latencyFaultHandler }{&lft}, args: args{"broker-a", 2500, false}},
		{name: "level7", fields: struct{ LatencyFaultHandler latencyFaultHandler }{&lft}, args: args{"broker-a", 3500, false}},
		{name: "level8", fields: struct{ LatencyFaultHandler latencyFaultHandler }{&lft}, args: args{"broker-a", 30000, false}},
		{name: "level9", fields: struct{ LatencyFaultHandler latencyFaultHandler }{&lft}, args: args{"broker-a", 20, true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &FaultStrategy{
				LatencyFaultHandler: tt.fields.LatencyFaultHandler,
			}
			fs.UpdateFaultItem(tt.args.brokerName, tt.args.currentLatency, tt.args.isolation)
			assert.False(t, fs.LatencyFaultHandler.IsAvailable(tt.args.brokerName))
		})
	}
}

func Test_computeIsolationDuration(t *testing.T) {
	type args struct {
		currentLatency int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{name: "level1", args: struct{ currentLatency int64 }{currentLatency: 20}, want: isolationDurationList[0]},
		{name: "level2", args: struct{ currentLatency int64 }{currentLatency: 500}, want: isolationDurationList[1]},
		{name: "level3", args: struct{ currentLatency int64 }{currentLatency: 800}, want: isolationDurationList[2]},
		{name: "level4", args: struct{ currentLatency int64 }{currentLatency: 1500}, want: isolationDurationList[3]},
		{name: "level5", args: struct{ currentLatency int64 }{currentLatency: 2500}, want: isolationDurationList[4]},
		{name: "level6", args: struct{ currentLatency int64 }{currentLatency: 3500}, want: isolationDurationList[5]},
		{name: "level7", args: struct{ currentLatency int64 }{currentLatency: 20000}, want: isolationDurationList[len(isolationDurationList)-1]},
		{name: "level8", args: struct{ currentLatency int64 }{currentLatency: isolationDefaultLatency}, want: isolationDurationList[len(isolationDurationList)-1]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := computeIsolationDuration(tt.args.currentLatency); got != tt.want {
				t.Errorf("computeIsolationDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}
