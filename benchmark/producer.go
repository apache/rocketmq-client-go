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

package main

import (
	"context"
	"flag"
	"github.com/bilinxing/rocketmq-client-go/v2"
	"github.com/bilinxing/rocketmq-client-go/v2/primitive"
	"github.com/bilinxing/rocketmq-client-go/v2/producer"
	"github.com/bilinxing/rocketmq-client-go/v2/rlog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type statiBenchmarkProducerSnapshot struct {
	sendRequestSuccessCount     int64
	sendRequestFailedCount      int64
	receiveResponseSuccessCount int64
	receiveResponseFailedCount  int64
	sendMessageSuccessTimeTotal int64
	sendMessageMaxRT            int64
	createdAt                   time.Time
	next                        *statiBenchmarkProducerSnapshot
}

type produceSnapshots struct {
	sync.RWMutex
	head, tail, cur *statiBenchmarkProducerSnapshot
	len             int
}

func (s *produceSnapshots) takeSnapshot() {
	b := s.cur
	sn := new(statiBenchmarkProducerSnapshot)
	sn.sendRequestSuccessCount = atomic.LoadInt64(&b.sendRequestSuccessCount)
	sn.sendRequestFailedCount = atomic.LoadInt64(&b.sendRequestFailedCount)
	sn.receiveResponseSuccessCount = atomic.LoadInt64(&b.receiveResponseSuccessCount)
	sn.receiveResponseFailedCount = atomic.LoadInt64(&b.receiveResponseFailedCount)
	sn.sendMessageSuccessTimeTotal = atomic.LoadInt64(&b.sendMessageSuccessTimeTotal)
	sn.sendMessageMaxRT = atomic.LoadInt64(&b.sendMessageMaxRT)
	sn.createdAt = time.Now()

	s.Lock()
	if s.tail != nil {
		s.tail.next = sn
	}
	s.tail = sn
	if s.head == nil {
		s.head = s.tail
	}

	s.len++
	if s.len > 10 {
		s.head = s.head.next
		s.len--
	}
	s.Unlock()
}

func (s *produceSnapshots) printStati() {
	s.RLock()
	if s.len < 10 {
		s.RUnlock()
		return
	}

	f, l := s.head, s.tail
	respSucCount := float64(l.receiveResponseSuccessCount - f.receiveResponseSuccessCount)
	sendTps := respSucCount / l.createdAt.Sub(f.createdAt).Seconds()
	avgRT := float64(l.sendMessageSuccessTimeTotal-f.sendMessageSuccessTimeTotal) / respSucCount
	maxRT := atomic.LoadInt64(&s.cur.sendMessageMaxRT)
	s.RUnlock()

	rlog.Info("Benchmark Producer Snapshot", map[string]interface{}{
		"sendTps":        int64(sendTps),
		"maxRt":          maxRT,
		"averageRt":      avgRT,
		"sendFailed":     l.sendRequestFailedCount,
		"responseFailed": l.receiveResponseFailedCount,
		"total":          l.receiveResponseSuccessCount,
	})
}

type producerBenchmark struct {
	topic         string
	nameSrv       string
	groupID       string
	instanceCount int
	testMinutes   int
	bodySize      int

	flags *flag.FlagSet
}

func init() {
	p := &producerBenchmark{}
	flags := flag.NewFlagSet("producer", flag.ExitOnError)
	p.flags = flags

	flags.StringVar(&p.topic, "t", "", "topic name")
	flags.StringVar(&p.nameSrv, "n", "", "nameserver address")
	flags.StringVar(&p.groupID, "g", "", "group id")
	flags.IntVar(&p.instanceCount, "i", 1, "instance count")
	flags.IntVar(&p.testMinutes, "m", 10, "test minutes")
	flags.IntVar(&p.bodySize, "s", 32, "body size")

	registerCommand("producer", p)
}

func (bp *producerBenchmark) produceMsg(stati *statiBenchmarkProducerSnapshot, exit chan struct{}) {
	p, err := rocketmq.NewProducer(
		producer.WithNameServer([]string{bp.nameSrv}),
		producer.WithRetry(2),
	)

	if err != nil {
		rlog.Error("New Producer Error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err.Error(),
		})
		return
	}

	err = p.Start()

	defer p.Shutdown()

	topic, tag := bp.topic, "benchmark-producer"
	msgStr := buildMsg(bp.bodySize)

AGAIN:
	select {
	case <-exit:
		return
	default:
	}

	now := time.Now()
	r, err := p.SendSync(context.Background(), primitive.NewMessage(topic, []byte(msgStr)))

	if err != nil {
		rlog.Error("Send Message Error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err.Error(),
		})
		goto AGAIN
	}

	if r.Status == primitive.SendOK {
		atomic.AddInt64(&stati.receiveResponseSuccessCount, 1)
		atomic.AddInt64(&stati.sendRequestSuccessCount, 1)
		currentRT := int64(time.Since(now) / time.Millisecond)
		atomic.AddInt64(&stati.sendMessageSuccessTimeTotal, currentRT)
		prevRT := atomic.LoadInt64(&stati.sendMessageMaxRT)
		for currentRT > prevRT {
			if atomic.CompareAndSwapInt64(&stati.sendMessageMaxRT, prevRT, currentRT) {
				break
			}
			prevRT = atomic.LoadInt64(&stati.sendMessageMaxRT)
		}
		goto AGAIN
	}
	rlog.Error("Send Message Error", map[string]interface{}{
		"topic":                  topic,
		"tag":                    tag,
		rlog.LogKeyUnderlayError: err.Error(),
	})
	goto AGAIN
}

func (bp *producerBenchmark) run(args []string) {
	bp.flags.Parse(args)

	if bp.topic == "" {
		rlog.Error("Empty Topic", nil)
		bp.flags.Usage()
		return
	}

	if bp.groupID == "" {
		rlog.Error("Empty Group Id", nil)
		bp.flags.Usage()
		return
	}

	if bp.nameSrv == "" {
		rlog.Error("Empty Nameserver", nil)
		bp.flags.Usage()
		return
	}
	if bp.instanceCount <= 0 {
		rlog.Error("Instance Count Must Be Positive Integer", nil)
		bp.flags.Usage()
		return
	}
	if bp.testMinutes <= 0 {
		rlog.Error("Test Time Must Be Positive Integer", nil)
		bp.flags.Usage()
		return
	}
	if bp.bodySize <= 0 {
		rlog.Error("Body Size Must Be Positive Integer", nil)
		bp.flags.Usage()
		return
	}

	stati := statiBenchmarkProducerSnapshot{}
	snapshots := produceSnapshots{cur: &stati}
	exitChan := make(chan struct{})
	wg := sync.WaitGroup{}

	for i := 0; i < bp.instanceCount; i++ {
		i := i
		go func() {
			wg.Add(1)
			bp.produceMsg(&stati, exitChan)
			rlog.Info("Producer Done and Exit", map[string]interface{}{
				"id": i,
			})
			wg.Done()
		}()
	}

	// snapshot
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				snapshots.takeSnapshot()
			case <-exitChan:
				ticker.Stop()
				return
			}
		}
	}()

	// print statistic
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second * 10)
		for {
			select {
			case <-ticker.C:
				snapshots.printStati()
			case <-exitChan:
				ticker.Stop()
				return
			}
		}
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-time.Tick(time.Minute * time.Duration(bp.testMinutes)):
	case <-signalChan:
	}

	close(exitChan)
	wg.Wait()
	snapshots.takeSnapshot()
	snapshots.printStati()
	rlog.Info("Test Done", nil)
}

func (bp *producerBenchmark) usage() {
	bp.flags.Usage()
}
