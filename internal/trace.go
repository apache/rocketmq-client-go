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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

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
		ss := strings.Split(ctx.GroupName, "%")
		if len(ss) == 2 {
			buffer.WriteString(ss[1])
		} else {
			buffer.WriteString(ctx.GroupName)
		}

		buffer.WriteRune(contentSplitter)
		ssTopic := strings.Split(bean.Topic, "%")
		if len(ssTopic) == 2 {
			buffer.WriteString(ssTopic[1])
		} else {
			buffer.WriteString(bean.Topic)
		}
		// buffer.WriteString(bean.Topic)
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
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(bean.ClientHost)
		buffer.WriteRune(fieldSplitter)
	case SubBefore:
		for _, bean := range ctx.TraceBeans {
			buffer.WriteString(string(ctx.TraceType))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(strconv.FormatInt(ctx.TimeStamp, 10))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(ctx.RegionId)
			buffer.WriteRune(contentSplitter)
			ss := strings.Split(ctx.GroupName, "%")
			if len(ss) == 2 {
				buffer.WriteString(ss[1])
			} else {
				buffer.WriteString(ctx.GroupName)
			}
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(ctx.RequestId)
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(bean.MsgId)
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(strconv.Itoa(bean.RetryTimes))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(nullWrap(bean.Keys))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(bean.ClientHost)
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
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(strconv.FormatInt(ctx.TimeStamp, 10))
			buffer.WriteRune(contentSplitter)
			ss := strings.Split(ctx.GroupName, "%")
			if len(ss) == 2 {
				buffer.WriteString(ss[1])
			} else {
				buffer.WriteString(ctx.GroupName)
			}
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
	// Keep 64-bit atomic data first for alignment on 32-bit platforms.
	discardCount    int64
	mu              sync.Mutex
	started, closed bool
	closeOnce       sync.Once
	ctx             context.Context
	cancel          context.CancelFunc
	sendCtx         context.Context
	cancelSend      context.CancelFunc
	processDone     chan struct{}
	closeDone       chan struct{}
	pending         sync.WaitGroup

	traceTopic string
	access     primitive.AccessChannel
	ticker     *time.Ticker
	input      chan TraceContext
	namesrvs   *namesrvs
	rrindex    int32
	cli        RMQClient
	resource   *traceClient
}

func NewTraceDispatcher(traceCfg *primitive.TraceConfig) *traceDispatcher {
	return newTraceDispatcher(traceCfg, nil)
}

func NewSharedTraceDispatcher(traceCfg *primitive.TraceConfig, shared primitive.SharedTraceClientConfig) *traceDispatcher {
	return newTraceDispatcher(traceCfg, &shared)
}

func newTraceDispatcher(traceCfg *primitive.TraceConfig, shared *primitive.SharedTraceClientConfig) *traceDispatcher {
	resource, err := acquireTraceClient(traceCfg, shared)
	if err != nil {
		rlog.Error("trace initialization failed; tracing is disabled", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil
	}
	topic := traceCfg.TraceTopic
	if topic == "" {
		topic = RmqSysTraceTopic
	}
	if traceCfg.Access == primitive.Cloud {
		topic = TraceTopicPrefix + traceCfg.TraceTopic
	}
	ctx, cancel := context.WithCancel(context.Background())
	sendCtx, cancelSend := context.WithCancel(context.Background())
	return &traceDispatcher{
		ctx: ctx, cancel: cancel, sendCtx: sendCtx, cancelSend: cancelSend,
		processDone: make(chan struct{}), closeDone: make(chan struct{}),
		traceTopic: topic, access: traceCfg.Access, input: make(chan TraceContext, 1024),
		cli: resource.cli, namesrvs: resource.namesrvs, resource: resource,
	}
}

func (td *traceDispatcher) GetTraceTopicName() string { return td.traceTopic }

func (td *traceDispatcher) Start() {
	if td == nil {
		return
	}
	td.mu.Lock()
	defer td.mu.Unlock()
	if td.started || td.closed {
		return
	}
	td.started = true
	maxWaitDuration := 5 * time.Millisecond
	td.ticker = time.NewTicker(maxWaitDuration)
	go primitive.WithRecover(func() {
		defer close(td.processDone)
		td.process(maxWaitDuration)
	})
}

func (td *traceDispatcher) Close() {
	if td == nil {
		return
	}
	td.closeOnce.Do(func() {
		td.mu.Lock()
		td.closed = true
		if td.started {
			td.ticker.Stop()
		} else {
			close(td.processDone)
		}
		td.cancel()
		td.mu.Unlock()
		// Drain accepted records, then wait for all asynchronous callbacks before
		// releasing a transport. If the deadline expires, cancel I/O and finish
		// cleanup in the background; never let a late send reopen a closed pool.
		go func() {
			<-td.processDone
			td.pending.Wait()
			td.cancelSend()
			td.resource.release()
			close(td.closeDone)
		}()
	})
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-td.closeDone:
	case <-timer.C:
		td.cancelSend()
	}
}

func (td *traceDispatcher) Append(ctx TraceContext) bool {
	if td == nil {
		return false
	}
	td.mu.Lock()
	defer td.mu.Unlock()
	if !td.started || td.closed {
		return false
	}
	select {
	case td.input <- ctx:
		return true
	default:
		rlog.Warning("trace buffer full", map[string]interface{}{
			"discardCount": atomic.AddInt64(&td.discardCount, 1),
		})
		return false
	}
}

func (td *traceDispatcher) submitBatch(batch []TraceContext) {
	if len(batch) == 0 {
		return
	}
	td.pending.Add(1)
	go primitive.WithRecover(func() {
		defer td.pending.Done()
		td.batchCommit(batch)
	})
}

func (td *traceDispatcher) process(maxWaitDuration time.Duration) {
	var batch []TraceContext
	lastPut := time.Now()
	flush := func() { td.submitBatch(batch); batch = nil }
	for {
		select {
		case ctx := <-td.input:
			lastPut = time.Now()
			batch = append(batch, ctx)
			if len(batch) == batchSize {
				flush()
			}
		case <-td.ticker.C:
			if time.Since(lastPut) > maxWaitDuration {
				lastPut = time.Now()
				flush()
			}
		case <-td.ctx.Done():
			// Append and Close share a lock, so no more records can enter.
			for {
				select {
				case ctx := <-td.input:
					batch = append(batch, ctx)
					if len(batch) == batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// batchCommit commit slice of TraceContext. convert the ctxs to keyed pair(key is Topic + regionid).
// flush according key one by one.
func (td *traceDispatcher) batchCommit(ctxs []TraceContext) {
	keyedCtxs := make(map[string][]TraceTransferBean)
	for _, ctx := range ctxs {
		if len(ctx.TraceBeans) == 0 {
			continue
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
	slice := make([]string, 0, len(ks))
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

func (td *traceDispatcher) sendTraceDataByMQ(keySet Keyset, regionID string, data string) {
	traceTopic := td.traceTopic
	if td.access == primitive.Cloud {
		traceTopic = td.traceTopic + regionID
	}
	msg := primitive.NewMessage(traceTopic, []byte(data))
	msg.WithKeys(keySet.slice())

	mq, addr := td.findMq(regionID)
	if mq == nil {
		return
	}

	var req = td.buildSendRequest(mq, msg)
	ctx, cancel := context.WithTimeout(td.sendCtx, 5*time.Second)
	td.pending.Add(1)
	var finishOnce sync.Once
	finish := func() { finishOnce.Do(func() { cancel(); td.pending.Done() }) }
	err := td.cli.InvokeAsync(ctx, addr, req, func(command *remote.RemotingCommand, e error) {
		defer finish()
		resp := primitive.NewSendResult()
		if e != nil {
			rlog.Info("send trace data error.", map[string]interface{}{
				"traceData": data,
			})
		} else {
			td.cli.ProcessSendResponse(mq.BrokerName, command, resp, msg)
			rlog.Debug("send trace data success:", map[string]interface{}{
				"SendResult": resp,
				"traceData":  data,
			})
		}
	})
	if err != nil {
		finish()
		rlog.Info("send trace data error when invoke", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
	}
}

func (td *traceDispatcher) findMq(regionID string) (*primitive.MessageQueue, string) {
	traceTopic := td.traceTopic
	if td.access == primitive.Cloud {
		traceTopic = td.traceTopic + regionID
	}
	if td.sendCtx.Err() != nil {
		return nil, ""
	}
	td.resource.topics.Store(traceTopic, struct{}{})
	// Each NameServer attempt has its own timeout; shutdown can still cancel
	// the whole lookup through sendCtx without cutting off healthy fallbacks.
	mqs, err := td.namesrvs.fetchPublishMessageQueuesWithContext(td.sendCtx, traceTopic)
	if err != nil {
		rlog.Error("fetch publish message queues failed", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, ""
	}
	if len(mqs) == 0 {
		rlog.Warning("could not fetch any publish message queue", map[string]interface{}{
			"topic": traceTopic,
		})
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
	addr := td.namesrvs.FindBrokerAddrByName(brokerName)

	return mq, addr
}

func (td *traceDispatcher) buildSendRequest(mq *primitive.MessageQueue,
	msg *primitive.Message) *remote.RemotingCommand {
	req := &SendMessageRequestHeader{
		ProducerGroup: TraceGroupName,
		Topic:         mq.Topic,
		QueueId:       mq.QueueId,
		BornTimestamp: time.Now().UnixNano() / int64(time.Millisecond),
		Flag:          msg.Flag,
		Properties:    msg.MarshallProperties(),
		BrokerName:    mq.BrokerName,
	}

	return remote.NewRemotingCommand(ReqSendMessage, req, msg.Body)
}
