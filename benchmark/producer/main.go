package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	rocketmq "github.com/apache/rocketmq-client-go/core"
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

type snapshots struct {
	sync.RWMutex
	head, tail, cur *statiBenchmarkProducerSnapshot
	len             int
}

func (s *snapshots) takeSnapshot() {
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

func (s *snapshots) printStati() {
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

	fmt.Printf(
		"Send TPS: %d Max RT: %d Average RT: %7.3f Send Failed: %d Response Failed: %d Total:%d\n",
		int64(sendTps), maxRT, avgRT, l.sendRequestFailedCount, l.receiveResponseFailedCount, l.receiveResponseSuccessCount,
	)
}

var (
	topic         string
	nameSrv       string
	groupID       string
	instanceCount int
	testMinutes   int
	bodySize      int

	longText    = ""
	longTextLen int
)

func init() {
	flag.StringVar(&topic, "t", "", "topic name")
	flag.StringVar(&nameSrv, "n", "", "nameserver address")
	flag.StringVar(&groupID, "g", "", "group id")
	flag.IntVar(&instanceCount, "i", 1, "instance count")
	flag.IntVar(&testMinutes, "m", 10, "test minutes")
	flag.IntVar(&bodySize, "s", 32, "body size")

	longText = strings.Repeat("0123456789", 100)
	longTextLen = len(longText)
}

func buildMsg() string {
	return longText[:bodySize]
}

func q(flag int) string {
	return fmt.Sprintf(queue+"_%d", flag)
}

func produceMsg(stati *statiBenchmarkProducerSnapshot, exit chan struct{}, topics, tags []string) {
	p, err := rocketmq.NewProducer(&rocketmq.ProducerConfig{
		GroupID:    "",
		NameServer: nameSrv,
	})
	if err != nil {
		fmt.Printf("new producer error:%s\n", err)
		return
	}

	p.Start()
	defer p.Shutdown()

AGAIN:
	select {
	case <-exit:
		return
	default:
	}

	for i := len(topics) - 1; i >= 0; i-- {
		topic := topics[i]
		for _, tag := range tags {
			req.Queue = topic
			req.Tags = []string{tag}
			req.Body = buildMsg()

			now := time.Now()
			ctx := context.Background()
			_, err := p.SendMessageSync(&rocketmq.Message{
				Topic: topic, Body: longText[:bodySize],
			})

			if err == nil {
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
				continue
			}

			fmt.Printf("%v send message %s:%s error:%s\n", time.Now(), topic, tag, err.Error())
			//if _, ok := err.(*rpc.ErrorInfo); ok { TODO
			//atomic.AddInt64(&stati.receiveResponseFailedCount, 1)
			//} else {
			//atomic.AddInt64(&stati.sendRequestFailedCount, 1)
			//}
		}
	}
	goto AGAIN
}

func takeSnapshot(s *snapshots, exit chan struct{}) {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			s.takeSnapshot()
		case <-exit:
			ticker.Stop()
			return
		}
	}
}

func printStati(s *snapshots, exit chan struct{}) {
	ticker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ticker.C:
			s.printStati()
		case <-exit:
			ticker.Stop()
			return
		}
	}
}

func main() {
	flag.Parse()

	stati := statiBenchmarkProducerSnapshot{}
	snapshots := snapshots{cur: &stati}
	exitChan := make(chan struct{})
	wg := sync.WaitGroup{}

	topics := make([]string, topicCount)
	for i := 0; i < topicCount; i++ {
		topics[i] = q(i + startSuffix)
	}

	tags := make([]string, tagCount)
	for i := 0; i < tagCount; i++ {
		tags[i] = fmt.Sprintf("default_tag_%d", i)
	}

	for i := 0; i < instanceCount; i++ {
		go func() {
			wg.Add(1)
			produceMsg(&stati, exitChan, topics, tags)
			fmt.Println("exit of produce ms")
			wg.Done()
		}()
	}

	// snapshot
	go func() {
		wg.Add(1)
		takeSnapshot(&snapshots, exitChan)
		wg.Done()
	}()

	// print statistic
	go func() {
		wg.Add(1)
		printStati(&snapshots, exitChan)
		wg.Done()
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-time.Tick(time.Minute * time.Duration(testMinutes)):
	case <-signalChan:
	}

	close(exitChan)
	wg.Wait()
	snapshots.takeSnapshot()
	snapshots.printStati()
	fmt.Println("TEST DONE")
}
