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
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type statiBenchmarkConsumerSnapshot struct {
	receiveMessageTotal   int64
	born2ConsumerTotalRT  int64
	store2ConsumerTotalRT int64
	born2ConsumerMaxRT    int64
	store2ConsumerMaxRT   int64
	createdAt             time.Time
	next                  *statiBenchmarkConsumerSnapshot
}

type consumeSnapshots struct {
	sync.RWMutex
	head, tail, cur *statiBenchmarkConsumerSnapshot
	len             int
}

func (s *consumeSnapshots) takeSnapshot() {
	b := s.cur
	sn := new(statiBenchmarkConsumerSnapshot)
	sn.receiveMessageTotal = atomic.LoadInt64(&b.receiveMessageTotal)
	sn.born2ConsumerMaxRT = atomic.LoadInt64(&b.born2ConsumerMaxRT)
	sn.born2ConsumerTotalRT = atomic.LoadInt64(&b.born2ConsumerTotalRT)
	sn.store2ConsumerMaxRT = atomic.LoadInt64(&b.store2ConsumerMaxRT)
	sn.store2ConsumerTotalRT = atomic.LoadInt64(&b.store2ConsumerTotalRT)
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

func (s *consumeSnapshots) printStati() {
	s.RLock()
	if s.len < 10 {
		s.RUnlock()
		return
	}

	f, l := s.head, s.tail
	respSucCount := float64(l.receiveMessageTotal - f.receiveMessageTotal)
	consumeTps := respSucCount / l.createdAt.Sub(f.createdAt).Seconds()
	avgB2CRT := float64(l.born2ConsumerTotalRT-f.born2ConsumerTotalRT) / respSucCount
	avgS2CRT := float64(l.store2ConsumerTotalRT-f.store2ConsumerTotalRT) / respSucCount
	s.RUnlock()

	rlog.Info("Benchmark Consumer Snapshot", map[string]interface{}{
		"consumeTPS":     int64(consumeTps),
		"average(B2C)RT": avgB2CRT,
		"average(S2C)RT": avgS2CRT,
		"max(B2C)RT":     l.born2ConsumerMaxRT,
		"max(S2C)RT":     l.store2ConsumerMaxRT,
	})
}

type consumerBenchmark struct {
	topic          string
	groupPrefix    string
	nameSrv        string
	isPrefixEnable bool
	filterType     string
	expression     string
	testMinutes    int
	instanceCount  int

	flags *flag.FlagSet

	groupID string
}

func init() {
	c := &consumerBenchmark{}
	flags := flag.NewFlagSet("consumer", flag.ExitOnError)
	c.flags = flags

	flags.StringVar(&c.topic, "t", "BenchmarkTest", "topic")
	flags.StringVar(&c.groupPrefix, "g", "benchmark_consumer", "group prefix")
	flags.StringVar(&c.nameSrv, "n", "", "namesrv address list, separated by comma")
	flags.BoolVar(&c.isPrefixEnable, "p", true, "group prefix is enable")
	flags.StringVar(&c.filterType, "f", "", "filter type,options:TAG|SQL92, or empty")
	flags.StringVar(&c.expression, "e", "*", "expression")
	flags.IntVar(&c.testMinutes, "m", 10, "test minutes")
	flags.IntVar(&c.instanceCount, "i", 1, "instance count")

	registerCommand("consumer", c)
}

func (bc *consumerBenchmark) consumeMsg(stati *statiBenchmarkConsumerSnapshot, exit chan struct{}) {
	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName(bc.groupID),
		consumer.WithNameServer([]string{bc.nameSrv}),
	)
	if err != nil {
		panic("new push consumer error:" + err.Error())
	}

	selector := consumer.MessageSelector{}
	err = c.Subscribe(bc.topic, selector, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			atomic.AddInt64(&stati.receiveMessageTotal, 1)
			now := time.Now().UnixNano() / int64(time.Millisecond)
			b2cRT := now - msg.BornTimestamp
			atomic.AddInt64(&stati.born2ConsumerTotalRT, b2cRT)
			s2cRT := now - msg.StoreTimestamp
			atomic.AddInt64(&stati.store2ConsumerTotalRT, s2cRT)

			for {
				old := atomic.LoadInt64(&stati.born2ConsumerMaxRT)
				if old >= b2cRT || atomic.CompareAndSwapInt64(&stati.born2ConsumerMaxRT, old, b2cRT) {
					break
				}
			}

			for {
				old := atomic.LoadInt64(&stati.store2ConsumerMaxRT)
				if old >= s2cRT || atomic.CompareAndSwapInt64(&stati.store2ConsumerMaxRT, old, s2cRT) {
					break
				}
			}
		}
		return consumer.ConsumeSuccess, nil
	})

	rlog.Info("Test Start", nil)
	c.Start()
	select {
	case <-exit:
		c.Shutdown()
		return
	}
}

func (bc *consumerBenchmark) run(args []string) {
	bc.flags.Parse(args)
	if bc.topic == "" {
		rlog.Error("Empty Topic", nil)
		bc.usage()
		return
	}

	if bc.groupPrefix == "" {
		rlog.Error("Empty Group Prefix", nil)
		bc.usage()
		return
	}

	if bc.nameSrv == "" {
		rlog.Error("Empty Nameserver", nil)
		bc.usage()
		return
	}

	if bc.testMinutes <= 0 {
		rlog.Error("Test Time Must Be Positive Integer", nil)
		bc.usage()
		return
	}

	if bc.instanceCount <= 0 {
		rlog.Error("Thread Count Must Be Positive Integer", nil)
		bc.usage()
		return
	}

	bc.groupID = bc.groupPrefix
	if bc.isPrefixEnable {
		bc.groupID += fmt.Sprintf("_%d", time.Now().UnixNano()/int64(time.Millisecond)%100)
	}

	stati := statiBenchmarkConsumerSnapshot{}
	snapshots := consumeSnapshots{cur: &stati}
	exitChan := make(chan struct{})

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		bc.consumeMsg(&stati, exitChan)
		wg.Done()
	}()

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
	case <-time.Tick(time.Minute * time.Duration(bc.testMinutes)):
	case <-signalChan:
	}

	close(exitChan)
	wg.Wait()
	snapshots.takeSnapshot()
	snapshots.printStati()
	rlog.Info("Test Done", nil)
}

func (bc *consumerBenchmark) usage() {
	bc.flags.Usage()
}
