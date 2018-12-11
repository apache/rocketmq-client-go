package main

import (
	"flag"
	"fmt"
	"sync"
	"sync/atomic"

	rocketmq "github.com/apache/rocketmq-client-go/core"
)

type queueSelectorByOrderID struct{}

func (s queueSelectorByOrderID) Select(size int, m *rocketmq.Message, arg interface{}) int {
	return arg.(int) % size
}

var (
	namesrvAddrs string
	topic        string
	body         string
	groupID      string
	msgCount     int64
	workerCount  int
)

func init() {
	flag.StringVar(&namesrvAddrs, "n", "", "name server address")
	flag.StringVar(&topic, "t", "", "topic")
	flag.StringVar(&groupID, "g", "", "group")
	flag.StringVar(&body, "d", "", "body")
	flag.Int64Var(&msgCount, "m", 0, "message count")
	flag.IntVar(&workerCount, "w", 0, "worker count")
}

type worker struct {
	p            rocketmq.Producer
	leftMsgCount *int64
}

func (w *worker) run() {
	selector := queueSelectorByOrderID{}
	for atomic.AddInt64(w.leftMsgCount, -1) >= 0 {
		r := w.p.SendMessageOrderly(
			&rocketmq.Message{Topic: topic, Body: body}, selector, 7 /*orderID*/, 3,
		)
		fmt.Printf("send result:%+v\n", r)
	}
}

// example:
// ./producer -n "localhost:9988" -t local_test -g local_test -d data -m 100 -w 10
func main() {
	flag.Parse()

	if namesrvAddrs == "" {
		println("empty namesrv address")
		return
	}

	if topic == "" {
		println("empty topic")
		return
	}

	if body == "" {
		println("empty body")
		return
	}

	if groupID == "" {
		println("empty groupID")
		return
	}

	if msgCount == 0 {
		println("zero message count")
		return
	}

	if workerCount == 0 {
		println("zero worker count")
		return
	}

	producer := rocketmq.NewProduer(&rocketmq.ProducerConfig{
		GroupID:    groupID,
		NameServer: namesrvAddrs,
	})
	producer.Start()
	defer producer.Shutdown()

	wg := sync.WaitGroup{}
	wg.Add(workerCount)

	workers := make([]worker, workerCount)
	for i := range workers {
		workers[i].p = producer
		workers[i].leftMsgCount = &msgCount
	}

	for i := range workers {
		go func(w *worker) { w.run(); wg.Done() }(&workers[i])
	}

	wg.Wait()
}
