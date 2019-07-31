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

package internal

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/rocketmq-client-go/internal/remote"
	"github.com/apache/rocketmq-client-go/internal/utils"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/apache/rocketmq-client-go/rlog"
)

var (
	counter        int16 = 0
	startTimestamp int64 = 0
	nextTimestamp  int64 = 0
	prefix         string
	locker         sync.Mutex
	classLoadId    int32 = 0
)

func init() {
	buf := new(bytes.Buffer)

	ip, err := utils.ClientIP4()
	if err != nil {
		ip = utils.FakeIP()
	}
	_, _ = buf.Write(ip)
	_ = binary.Write(buf, binary.BigEndian, Pid())
	_ = binary.Write(buf, binary.BigEndian, classLoadId)
	prefix = strings.ToUpper(hex.EncodeToString(buf.Bytes()))
}

func CreateUniqID() string {
	locker.Lock()
	defer locker.Unlock()

	if time.Now().Unix() > nextTimestamp {
		updateTimestamp()
	}
	counter++
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, int32((time.Now().Unix()-startTimestamp)*1000))
	_ = binary.Write(buf, binary.BigEndian, counter)

	return prefix + hex.EncodeToString(buf.Bytes())
}

func updateTimestamp() {
	year, month := time.Now().Year(), time.Now().Month()
	startTimestamp = time.Date(year, month, 1, 0, 0, 0, 0, time.Local).Unix()
	nextTimestamp = time.Date(year, month, 1, 0, 0, 0, 0, time.Local).AddDate(0, 1, 0).Unix()
}

func Pid() int16 {
	return int16(os.Getpid())
}

type TraceBean struct {
	Topic       string
	MsgId       string
	OffsetMsgId string
	Tags        string
	Keys        string
	StoreHost   string
	ClientHost  string
	StoreTime   int64
	RetryTimes  int
	BodyLength  int
	MsgType     primitive.MessageType
}

type TraceTransferBean struct {
	transData string
	// not duplicate
	transKey []string
}

type TraceType string

const (
	Pub       TraceType = "Pub"
	SubBefore TraceType = "SubBefore"
	SubAfter  TraceType = "SubAfter"

	contentSplitter = '\001'
	fieldSplitter   = '\002'
)

type TraceContext struct {
	TraceType   TraceType
	TimeStamp   int64
	RegionId    string
	RegionName  string
	GroupName   string
	CostTime    int64
	IsSuccess   bool
	RequestId   string
	ContextCode int
	TraceBeans  []TraceBean
}

func (ctx *TraceContext) marshal2Bean() *TraceTransferBean {
	buffer := bytes.NewBufferString("")
	switch ctx.TraceType {
	case Pub:
		bean := ctx.TraceBeans[0]
		buffer.WriteString(string(ctx.TraceType))
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(strconv.FormatInt(ctx.TimeStamp, 10))
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(ctx.RegionId)
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(ctx.GroupName)
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(bean.Topic)
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(bean.MsgId)
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(bean.Tags)
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(bean.Keys)
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(bean.StoreHost)
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(strconv.Itoa(bean.BodyLength))
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(strconv.FormatInt(ctx.CostTime, 10))
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(strconv.Itoa(int(bean.MsgType)))
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(bean.OffsetMsgId)
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(strconv.FormatBool(ctx.IsSuccess))
		buffer.WriteRune(fieldSplitter)
	case SubBefore:
		for _, bean := range ctx.TraceBeans {
			buffer.WriteString(string(ctx.TraceType))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(strconv.FormatInt(ctx.TimeStamp, 10))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(ctx.RegionId)
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(ctx.GroupName)
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(ctx.RequestId)
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(bean.MsgId)
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(strconv.Itoa(bean.RetryTimes))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(nullWrap(bean.Keys))
			buffer.WriteRune(fieldSplitter)
		}
	case SubAfter:
		for _, bean := range ctx.TraceBeans {
			buffer.WriteString(string(ctx.TraceType))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(ctx.RequestId)
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(bean.MsgId)
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(strconv.FormatInt(ctx.CostTime, 10))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(strconv.FormatBool(ctx.IsSuccess))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(nullWrap(bean.Keys))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(strconv.Itoa(ctx.ContextCode))
			buffer.WriteRune(fieldSplitter)
		}
	}
	transferBean := new(TraceTransferBean)
	transferBean.transData = buffer.String()
	for _, bean := range ctx.TraceBeans {
		transferBean.transKey = append(transferBean.transKey, bean.MsgId)
		if len(bean.Keys) > 0 {
			transferBean.transKey = append(transferBean.transKey, bean.Keys)
		}
	}
	return transferBean
}

// compatible with java console.
func nullWrap(s string) string {
	if len(s) == 0 {
		return "null"
	}
	return s
}

type traceDispatcherType int

const (
	RmqSysTraceTopic = "RMQ_SYS_TRACE_TOPIC"

	ProducerType traceDispatcherType = iota
	ConsumerType

	maxMsgSize = 128000 - 10*1000
	batchSize  = 100

	TraceTopicPrefix = SystemTopicPrefix + "TRACE_DATA_"
	TraceGroupName   = "_INNER_TRACE_PRODUCER"
)

type TraceDispatcher interface {
	GetTraceTopicName() string

	Start()
	Append(ctx TraceContext) bool
	Close()
}

type traceDispatcher struct {
	ctx     context.Context
	cancel  context.CancelFunc
	running bool

	traceTopic string
	access     primitive.AccessChannel

	ticker  *time.Ticker
	input   chan TraceContext
	batchCh chan []*TraceContext

	discardCount int64

	// support deliver trace message to other cluster.
	namesrvs []string
	// round robin index
	rrindex int32
	cli     RMQClient
}

func NewTraceDispatcher(traceTopic string, access primitive.AccessChannel) *traceDispatcher {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	t := traceTopic
	if len(t) == 0 {
		t = RmqSysTraceTopic
	}

	if access == primitive.Cloud {
		t = TraceTopicPrefix + traceTopic
	}

	cliOp := DefaultClientOptions()
	cliOp.RetryTimes = 0
	cli := GetOrNewRocketMQClient(cliOp)
	return &traceDispatcher{
		ctx:    ctx,
		cancel: cancel,

		traceTopic: t,
		access:     access,
		input:      make(chan TraceContext, 1024),
		batchCh:    make(chan []*TraceContext, 2048),
		cli:        cli,
	}
}

func (td *traceDispatcher) GetTraceTopicName() string {
	return td.traceTopic
}

func (td *traceDispatcher) Start() {
	td.running = true
	td.cli.Start()
	go td.process()
}

func (td *traceDispatcher) Close() {
	td.running = false
	td.ticker.Stop()
	td.cancel()
}

func (td *traceDispatcher) Append(ctx TraceContext) bool {
	if !td.running {
		rlog.Error("traceDispatcher is closed.")
		return false
	}
	select {
	case td.input <- ctx:
		return true
	default:
		rlog.Warnf("buffer full: %d, ctx is %v", atomic.AddInt64(&td.discardCount, 1), ctx)
		return false
	}
}

// process
func (td *traceDispatcher) process() {
	var count int
	var batch []TraceContext
	maxWaitDuration := 5 * time.Millisecond
	maxWaitTime := maxWaitDuration.Nanoseconds()
	td.ticker = time.NewTicker(maxWaitDuration)
	lastput := time.Now()
	for {
		select {
		case ctx := <-td.input:
			count++
			lastput = time.Now()
			batch = append(batch, ctx)
			if count == batchSize {
				count = 0
				go td.batchCommit(batch)
				batch = make([]TraceContext, 0)
			}
		case <-td.ticker.C:
			delta := time.Since(lastput).Nanoseconds()
			if delta > maxWaitTime {
				count++
				lastput = time.Now()
				if len(batch) > 0 {
					go td.batchCommit(batch)
					batch = make([]TraceContext, 0)
				}
			}
		case <-td.ctx.Done():
			go td.batchCommit(batch)
			batch = make([]TraceContext, 0)

			now := time.Now().UnixNano() / int64(time.Millisecond)
			end := now + 500
			for now < end {
				now = time.Now().UnixNano() / int64(time.Millisecond)
				runtime.Gosched()
			}
			rlog.Infof("------end trace send %v %v", td.input, td.batchCh)
		}
	}
}

// batchCommit commit slice of TraceContext. convert the ctxs to keyed pair(key is Topic + regionid).
// flush according key one by one.
func (td *traceDispatcher) batchCommit(ctxs []TraceContext) {
	keyedCtxs := make(map[string][]TraceTransferBean)
	for _, ctx := range ctxs {
		if len(ctx.TraceBeans) == 0 {
			return
		}
		topic := ctx.TraceBeans[0].Topic
		regionID := ctx.RegionId
		key := topic
		if len(regionID) > 0 {
			key = fmt.Sprintf("%s%c%s", topic, contentSplitter, regionID)
		}
		keyedCtxs[key] = append(keyedCtxs[key], *ctx.marshal2Bean())
	}

	for k, v := range keyedCtxs {
		arr := strings.Split(k, string([]byte{contentSplitter}))
		topic := k
		regionID := ""
		if len(arr) > 1 {
			topic = arr[0]
			regionID = arr[1]
		}
		td.flush(topic, regionID, v)
	}
}

type Keyset map[string]struct{}

func (ks Keyset) slice() []string {
	slice := make([]string, len(ks))
	for k, _ := range ks {
		slice = append(slice, k)
	}
	return slice
}

// flush data in batch.
func (td *traceDispatcher) flush(topic, regionID string, data []TraceTransferBean) {
	if len(data) == 0 {
		return
	}

	keyset := make(Keyset)
	var builder strings.Builder
	flushed := true
	for _, bean := range data {
		for _, k := range bean.transKey {
			keyset[k] = struct{}{}
		}
		builder.WriteString(bean.transData)
		flushed = false

		if builder.Len() > maxMsgSize {
			td.sendTraceDataByMQ(keyset, regionID, builder.String())
			builder.Reset()
			keyset = make(Keyset)
			flushed = true
		}
	}
	if !flushed {
		td.sendTraceDataByMQ(keyset, regionID, builder.String())
	}
}

func (td *traceDispatcher) sendTraceDataByMQ(keyset Keyset, regionID string, data string) {
	msg := primitive.NewMessage(td.traceTopic, []byte(data))
	msg.SetKeys(keyset.slice())

	mq, addr := td.findMq()
	if mq == nil {
		return
	}

	var req = td.buildSendRequest(mq, msg)
	td.cli.InvokeAsync(addr, req, 5000*time.Millisecond, func(command *remote.RemotingCommand, e error) {
		if e != nil {
			rlog.Error("send trace data ,the traceData is %v", data)
		}
	})
}

func (td *traceDispatcher) findMq() (*primitive.MessageQueue, string) {
	mqs, err := FetchPublishMessageQueues(td.traceTopic)
	if err != nil {
		rlog.Error("fetch publish message queues failed. err: %v", err)
		return nil, ""
	}
	i := atomic.AddInt32(&td.rrindex, 1)
	if i < 0 {
		i = 0
		atomic.StoreInt32(&td.rrindex, 0)
	}
	i %= int32(len(mqs))
	mq := mqs[i]

	brokerName := mq.BrokerName
	addr := FindBrokerAddrByName(brokerName)

	return mq, addr
}

func (td *traceDispatcher) buildSendRequest(mq *primitive.MessageQueue,
	msg *primitive.Message) *remote.RemotingCommand {
	req := &SendMessageRequest{
		ProducerGroup: TraceGroupName,
		Topic:         mq.Topic,
		QueueId:       mq.QueueId,
		BornTimestamp: time.Now().UnixNano() / int64(time.Millisecond),
		Flag:          msg.Flag,
		Properties:    primitive.MarshalPropeties(msg.Properties),
	}

	return remote.NewRemotingCommand(ReqSendMessage, req, msg.Body)
}
