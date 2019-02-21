# Apache RocketMQ Golang Client Test Case List

## Unit Test
**TODO: How to mock cgo API**

### core package
#### api.go
- [ ] `func (config *ClientConfig) String() string`
- [x] `func (config *ProducerConfig) String() string`
- [x] `func (mode MessageModel) String() string`
- [x] `func (config *PushConsumerConfig) String() string`
- [ ] `func (config *PullConsumerConfig) String() string`
- [ ] `func (session *SessionCredentials) String() string`
- [ ] `func (result *SendResult) String() string`

#### cfuns.go
- [ ] `func consumeMessageCallback(cconsumer *C.CPushConsumer, msg *C.CMessageExt) C.int `

#### error.go
- [ ] `func (e rmqError) Error() string`

#### log.go
- [x] `func (l LogLevel) String() string `
- [x] `func (lc *LogConfig) String() string `

#### message.go
- [ ] `func (msg *Message) String() string`
- [ ] `func goMsgToC(gomsg *Message) *C.struct_CMessage `
- [ ] `func (msgExt *MessageExt) String() string`
- [ ] `func (msgExt *MessageExt) GetProperty(key string) string `
- [ ] `func cmsgExtToGo(cmsg *C.struct_CMessageExt) *MessageExt `

#### producer.go
- [ ] `func (status SendStatus) String() string `
- [ ] `func newDefaultProducer(config *ProducerConfig) (*defaultProducer, error)`
- [ ] `func (p *defaultProducer) String() string `
- [ ] `func (p *defaultProducer) Start() error `
- [ ] `func (p *defaultProducer) Shutdown() error `
- [ ] `func (p *defaultProducer) SendMessageSync(msg *Message) (*SendResult, error)`
- [ ] `func (p *defaultProducer) SendMessageOrderly(msg *Message, selector MessageQueueSelector, arg interface{}, autoRetryTimes int) (*SendResult, error) `
- [ ] `func (p *defaultProducer) SendMessageOneway(msg *Message) error `

#### pull_consumer.go
- [ ] `func (ps PullStatus) String() string `
- [ ] `func (c *defaultPullConsumer) String() string `
- [ ] `func NewPullConsumer(config *PullConsumerConfig) (PullConsumer, error) `
- [ ] `func (c *defaultPullConsumer) Start() error `
- [ ] `func (c *defaultPullConsumer) Shutdown() error `
- [ ] `func (c *defaultPullConsumer) FetchSubscriptionMessageQueues(topic string) []MessageQueue `
- [ ] `func (pr *PullResult) String() string `
- [ ] `func (c *defaultPullConsumer) Pull(mq MessageQueue, subExpression string, offset int64, maxNums int) PullResult `

#### push_consumer.go
- [ ] `func (status ConsumeStatus) String() string `
- [ ] `func (c *defaultPushConsumer) String() string `
- [ ] `func newPushConsumer(config *PushConsumerConfig) (PushConsumer, error) `
- [ ] `func (c *defaultPushConsumer) Start() error `
- [ ] `func (c *defaultPushConsumer) Shutdown() error `
- [ ] `func (c *defaultPushConsumer) Subscribe(topic, expression string, consumeFunc func(msg *MessageExt) ConsumeStatus) error `

#### queue_selector.go
- [ ] `func (q *MessageQueue) String() string `
- [ ] `func queueSelectorCallback(size int, selectorKey unsafe.Pointer) int `
- [x] `func (w *messageQueueSelectorWrapper) Select(size int) int`
- [x] `func (s *selectorHolder) put(selector *messageQueueSelectorWrapper) (key int) `
- [x] `func (s *selectorHolder) getAndDelete(key int) (*messageQueueSelectorWrapper, bool) `

## Integration Test
### Producer
- send message with `DelayTimeLevel`
- send message with `tag`
- send message orderly with `tag`

### Consumer
- consume message delayed through push/pull
- subscribe topic with expression 
- pull message with expression