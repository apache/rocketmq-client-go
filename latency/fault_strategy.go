package latency

// latency range max limit milliseconds
var latencyMaxList = []int64{50, 100, 550, 1000, 2000, 3000, 15000}

// mapped isolate milliseconds with latency range
var isolationDurationList = []int64{0, 0, 30000, 60000, 120000, 180000, 600000}

var isolationDefaultLatency int64 = 30000

type FaultStrategy struct {
	LatencyFaultHandler latencyFaultHandler
}

func NewDefaultFaultStrategy() *FaultStrategy {
	var lft latencyFaultTolerance
	return &FaultStrategy{LatencyFaultHandler: &lft}
}

// update broker latency info by fault strategy
func (fs *FaultStrategy) UpdateFaultItem(brokerName string, currentLatency int64, isolation bool) {

	if isolation {
		currentLatency = isolationDefaultLatency
	}
	isolationDuration := computeIsolationDuration(currentLatency)
	fs.LatencyFaultHandler.UpdateFaultItem(brokerName, currentLatency, isolationDuration)

}

// compute the duration of broker isolation from rage
func computeIsolationDuration(currentLatency int64) int64 {
	for i := len(latencyMaxList) - 1; i >= 0; i-- {
		if currentLatency >= latencyMaxList[i] {
			return isolationDurationList[i]
		}
	}
	return 0
}
