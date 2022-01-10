package latency

import (
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
	"sync"
	"testing"
	"time"
)

func Test_latencyFaultTolerance_UpdateFaultItem(t *testing.T) {

	var fim sync.Map
	fim.Store("broker-a", faultItem{
		name:              "broker-a",
		currentLatency:    atomic.NewInt64(300),
		releasedTimestamp: atomic.NewInt64(3000 + time.Now().UnixNano()/1e6),
	})

	type fields struct {
		faultItemMap sync.Map
	}
	type args struct {
		brokerName        string
		currentLatency    int64
		isolationDuration int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{name: "newFaultItem", fields: struct{ faultItemMap sync.Map }{}, args: args{"broker-a", 3000, 3000}},
		{name: "existFaultItem", fields: struct{ faultItemMap sync.Map }{fim}, args: args{"broker-a", 200, 10000}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lft := &latencyFaultTolerance{
				faultItemMap: tt.fields.faultItemMap,
			}
			flm, exist := lft.faultItemMap.Load(tt.args.brokerName)
			if tt.name == "newFaultItem" {
				assert.False(t, exist)
			} else if tt.name == "existFaultItem" {
				assert.True(t, exist)
			}
			lft.UpdateFaultItem(tt.args.brokerName, tt.args.currentLatency, tt.args.isolationDuration)
			flm, exist = lft.faultItemMap.Load(tt.args.brokerName)
			assert.True(t, exist)
			assert.Equal(t, tt.args.currentLatency, flm.(faultItem).currentLatency.Load())
			assert.True(t, tt.args.isolationDuration+time.Now().UnixNano()/1e6 >= flm.(faultItem).releasedTimestamp.Load())
		})
	}
}

func Test_latencyFaultTolerance_IsAvailable(t *testing.T) {

	var fim sync.Map
	fim.Store("broker-a", faultItem{
		name:              "broker-a",
		currentLatency:    atomic.NewInt64(300),
		releasedTimestamp: atomic.NewInt64(30000 + time.Now().UnixNano()/1e6),
	})

	type fields struct {
		faultItemMap sync.Map
	}
	type args struct {
		brokerName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{name: "notAvailable", fields: struct{ faultItemMap sync.Map }{fim}, args: args{"broker-a"}, want: false},
		{name: "available", fields: struct{ faultItemMap sync.Map }{fim}, args: args{"broker-b"}, want: true}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lft := &latencyFaultTolerance{
				faultItemMap: tt.fields.faultItemMap,
			}
			if got := lft.IsAvailable(tt.args.brokerName); got != tt.want {
				t.Errorf("IsAvailable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_latencyFaultTolerance_Remove(t *testing.T) {

	var fim sync.Map
	fim.Store("broker-a", faultItem{
		name:              "broker-a",
		currentLatency:    atomic.NewInt64(300),
		releasedTimestamp: atomic.NewInt64(30000 + time.Now().UnixNano()/1e6),
	})

	type fields struct {
		faultItemMap sync.Map
	}
	type args struct {
		brokerName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{name: "removeBrokerA", fields: struct{ faultItemMap sync.Map }{}, args: args{"broker-a"}},
		{name: "removeFromEmptyMap", fields: struct{ faultItemMap sync.Map }{fim}, args: args{"broker-b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lft := &latencyFaultTolerance{
				faultItemMap: tt.fields.faultItemMap,
			}
			_, exist := lft.faultItemMap.Load(tt.args.brokerName)
			lft.faultItemMap.Delete(tt.args.brokerName)
			_, exist = lft.faultItemMap.Load(tt.args.brokerName)
			assert.False(t, exist)
		})
	}
}

func Test_latencyFaultTolerance_PickOneAtLeast(t *testing.T) {

	var fim sync.Map
	fim.Store("broker-a", faultItem{
		name:              "broker-a",
		currentLatency:    atomic.NewInt64(300),
		releasedTimestamp: atomic.NewInt64(30000 + time.Now().UnixNano()/1e6),
	})
	fim.Store("broker-b", faultItem{
		name:              "broker-b",
		currentLatency:    atomic.NewInt64(300),
		releasedTimestamp: atomic.NewInt64(60000 + time.Now().UnixNano()/1e6),
	})

	var fimEmpty sync.Map

	type fields struct {
		faultItemMap sync.Map
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{name: "pickEarliestBroker", fields: struct{ faultItemMap sync.Map }{fim}, want: "broker-a"},
		{name: "pickEmpty", fields: struct{ faultItemMap sync.Map }{fimEmpty}, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lft := &latencyFaultTolerance{
				faultItemMap: tt.fields.faultItemMap,
			}
			if got := lft.PickOneAtLeast(); got != tt.want {
				t.Errorf("PickOneAtLeast() = %v, want %v", got, tt.want)
			}
		})
	}
}
