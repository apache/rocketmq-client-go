package latency

import (
	"go.uber.org/atomic"
	"sync"
	"time"
)

// interface for handle mq fault
type latencyFaultHandler interface {

	// update wrong item when catch it
	UpdateFaultItem(brokerName string, currentLatency int64, isolationDuration int64)

	IsAvailable(brokerName string) bool

	Remove(brokerName string)

	PickOneAtLeast() string
}

// fault tolerance handler
type latencyFaultTolerance struct {

	// brokerName -> faultItem
	faultItemMap sync.Map
}

type faultItem struct {
	name string

	currentLatency *atomic.Int64

	// timestamp to release broker
	releasedTimestamp *atomic.Int64
}

// update broker latency info
func (lft *latencyFaultTolerance) UpdateFaultItem(brokerName string, currentLatency int64, isolationDuration int64) {
	fi, exist := lft.faultItemMap.Load(brokerName)
	if exist {
		faultItem := fi.(faultItem)
		faultItem.currentLatency.Store(currentLatency)
		faultItem.releasedTimestamp.Store(time.Now().UnixNano()/int64(time.Millisecond) + isolationDuration)
	} else {
		lft.faultItemMap.LoadOrStore(brokerName, faultItem{
			name:              brokerName,
			currentLatency:    atomic.NewInt64(currentLatency),
			releasedTimestamp: atomic.NewInt64(time.Now().UnixNano()/int64(time.Millisecond) + isolationDuration),
		})
	}
}

// check broker is available,when
func (lft *latencyFaultTolerance) IsAvailable(brokerName string) bool {
	fi, exist := lft.faultItemMap.Load(brokerName)
	if exist {
		return time.Now().UnixNano()/int64(time.Millisecond)-fi.(faultItem).releasedTimestamp.Load() >= 0
	} else {
		return true
	}
}

func (lft *latencyFaultTolerance) Remove(brokerName string) {
	lft.faultItemMap.Delete(brokerName)
}

// pick the earliest isolate broker
func (lft *latencyFaultTolerance) PickOneAtLeast() string {

	var minIsolateTimestamp int64 = 0
	var luckyBrokerName = ""
	lft.faultItemMap.Range(func(brokerName, fi interface{}) bool {
		releasedTimestamp := fi.(faultItem).releasedTimestamp.Load()
		if minIsolateTimestamp == 0 || minIsolateTimestamp > releasedTimestamp {
			minIsolateTimestamp = releasedTimestamp
			luckyBrokerName = brokerName.(string)
		}
		return true
	})
	return luckyBrokerName
}
