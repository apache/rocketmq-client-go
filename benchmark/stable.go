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
	"flag"
	"github.com/apache/rocketmq-client-go/v2/errors"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type stableTest struct {
	nameSrv       string
	topic         string
	groupID       string
	opIntervalSec int
	testMin       int

	op func()

	flags *flag.FlagSet
}

func (st *stableTest) buildFlags(name string) {
	flags := flag.NewFlagSet(name, flag.ExitOnError)
	flags.StringVar(&st.topic, "t", "stable-test", "topic name")
	flags.StringVar(&st.nameSrv, "n", "", "nameserver address")
	flags.StringVar(&st.groupID, "g", "stable-test", "group id")
	flags.IntVar(&st.testMin, "m", 10, "test minutes")
	flags.IntVar(&st.opIntervalSec, "s", 1, "operation interval[produce/consume]")

	st.flags = flags
}

func (st *stableTest) checkFlag() error {
	if st.topic == "" {
		return errors.ErrEmptyTopic
	}

	if st.nameSrv == "" {
		return errors.ErrEmptyNameSrv
	}

	if st.groupID == "" {
		return errors.ErrEmptyGroupID
	}

	if st.testMin <= 0 {
		return errors.ErrTestMin
	}

	if st.opIntervalSec <= 0 {
		return errors.ErrOperationInterval
	}

	return nil
}

func (st *stableTest) run() {
	opTicker := time.NewTicker(time.Duration(st.opIntervalSec) * time.Second)
	closeChan := time.Tick(time.Duration(st.testMin) * time.Minute)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-signalChan:
			opTicker.Stop()
			rlog.Info("Test Done", nil)
			return
		case <-closeChan:
			opTicker.Stop()
			rlog.Info("Test Done", nil)
			return
		case <-opTicker.C:
			st.op()
		}
	}
}

type stableTestProducer struct {
	*stableTest
	bodySize int

	//p rocketmq.Producer
}

func (stp *stableTestProducer) buildFlags(name string) {
	stp.stableTest.buildFlags(name)
	stp.flags.IntVar(&stp.bodySize, "b", 32, "body size")
}

func (stp *stableTestProducer) checkFlag() error {
	err := stp.stableTest.checkFlag()
	if err != nil {
		return err
	}
	if stp.bodySize <= 0 {
		return errors.ErrMessageBody
	}

	return nil
}

func (stp *stableTestProducer) usage() {
	stp.flags.Usage()
}

func (stp *stableTestProducer) run(args []string) {
	err := stp.flags.Parse(args)
	if err != nil {
		rlog.Info("Parse Args Error", map[string]interface{}{
			"args":                   args,
			rlog.LogKeyUnderlayError: err.Error(),
		})
		stp.usage()
		return
	}

	err = stp.checkFlag()
	if err != nil {
		rlog.Error("Check Flag Error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err.Error(),
		})
		stp.usage()
		return
	}

	//p, err := rocketmq.NewProducer(&rocketmq.ProducerConfig{
	//	ClientConfig: rocketmq.ClientConfig{GroupID: stp.groupID, NameServer: stp.nameSrv},
	//})
	//if err != nil {
	//	fmt.Printf("new consumer error:%s\n", err)
	//	return
	//}
	//
	//err = p.Start()
	//if err != nil {
	//	fmt.Printf("start consumer error:%s\n", err)
	//	return
	//}
	//defer p.Shutdown()
	//
	//stp.p = p
	stp.stableTest.run()
}

func (stp *stableTestProducer) sendMessage() {
	//r, err := stp.p.SendMessageSync(&rocketmq.Message{Topic: stp.topic, Body: buildMsg(stp.bodySize)})
	//if err == nil {
	//	fmt.Printf("send result:%+v\n", r)
	//	return
	//}
	//fmt.Printf("send message error:%s", err)
}

type stableTestConsumer struct {
	*stableTest
	expression string

	//c       rocketmq.PullConsumer
	offsets map[int]int64
}

func (stc *stableTestConsumer) buildFlags(name string) {
	stc.stableTest.buildFlags(name)
	stc.flags.StringVar(&stc.expression, "e", "*", "expression")
}

func (stc *stableTestConsumer) checkFlag() error {
	err := stc.stableTest.checkFlag()
	if err != nil {
		return err
	}

	if stc.expression == "" {
		return errors.ErrEmptyExpression
	}
	return nil
}

func (stc *stableTestConsumer) usage() {
	stc.flags.Usage()
}

func (stc *stableTestConsumer) run(args []string) {
	err := stc.flags.Parse(args)
	if err != nil {
		rlog.Error("Parse Args Error", map[string]interface{}{
			"args":                   args,
			rlog.LogKeyUnderlayError: err.Error(),
		})
		stc.usage()
		return
	}

	err = stc.checkFlag()
	if err != nil {
		rlog.Error("Check Flag Error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err.Error(),
		})
		stc.usage()
		return
	}
	//
	//c, err := rocketmq.NewPullConsumer(&rocketmq.PullConsumerConfig{
	//	ClientConfig: rocketmq.ClientConfig{GroupID: stc.groupID, NameServer: stc.nameSrv},
	//})
	//if err != nil {
	//	fmt.Printf("new pull consumer error:%s\n", err)
	//	return
	//}
	//
	//err = c.Start()
	//if err != nil {
	//	fmt.Printf("start consumer error:%s\n", err)
	//	return
	//}
	//defer c.Shutdown()
	//
	//stc.c = c
	stc.stableTest.run()
}

func (stc *stableTestConsumer) pullMessage() {
	//mqs := stc.c.FetchSubscriptionMessageQueues(stc.topic)
	//
	//for _, mq := range mqs {
	//	offset := stc.offsets[mq.ID]
	//	pr := stc.c.Pull(mq, stc.expression, offset, 32)
	//fmt.Printf("pull from %s, offset:%d, count:%+v\n", mq.String(), offset, len(pr.Messages))
	//
	//switch pr.Status {
	//case rocketmq.PullNoNewMsg:
	//	stc.offsets[mq.ID] = 0 // pull from the begin
	//case rocketmq.PullFound:
	//	fallthrough
	//case rocketmq.PullNoMatchedMsg:
	//	fallthrough
	//case rocketmq.PullOffsetIllegal:
	//	stc.offsets[mq.ID] = pr.NextBeginOffset
	//case rocketmq.PullBrokerTimeout:
	//	fmt.Println("broker timeout occur")
	//}
	//}
}

func init() {
	// consumer
	name := "stableTestProducer"
	p := &stableTestProducer{stableTest: &stableTest{}}
	p.buildFlags(name)
	p.op = p.sendMessage
	registerCommand(name, p)

	// consumer
	name = "stableTestConsumer"
	c := &stableTestConsumer{stableTest: &stableTest{}, offsets: map[int]int64{}}
	c.buildFlags(name)
	c.op = c.pullMessage
	registerCommand(name, c)
}
