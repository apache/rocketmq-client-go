----------
## RocketMQ Client Go

### 1. Go Version
* go1.10.5 darwin/amd64


### 2. Dependency
* [librocketmq](https://github.com/apache/rocketmq-client-cpp)	

### 3. Build and Install
#### macOS Platform (macOS Mojave 10.14)
* Install Compile tools (homebrew package manager)
    ```
    1. xcode-select --install
    2. brew install cmake
    3. brew install automake
    ```
* Install dependencies
    1. [Go official download](https://golang.org/dl/)

    2. Get go client package
    ```
    go get github.com/apache/rocketmq-client-go
    ```
    3. [librocketmq](https://github.com/apache/rocketmq-client-cpp)
        - `git clone https://github.com/apache/rocketmq-client-cpp`
        - `cd rocketmq-client-cpp`
        - `sudo sh build.sh` 
        - `sudo install bin/librocketmq.dylib /usr/local/lib`
        - `sudo mkdir /usr/local/include/rocketmq`
        - `sudo cp include/* /usr/local/include/rocketmq/`

#### Linux

*coming soon*

#### Windows

*coming soon*

----------
## How to use

- import package
    ```
    import rocketmq "github.com/apache/rocketmq-client-go/core"
    ```
- Send message
    ```go
    func SendMessagge(){
        producer := rocketmq.NewProducer(&rocketmq.ProducerConfig{GroupID: "testGroup", NameServer: "localhost:9876"})
        producer.Start()
        defer producer.Shutdown()
        fmt.Printf("Producer: %s started... \n", producer)
	    for i := 0; i < 100; i++ {
		    msg := fmt.Sprintf("Hello RocketMQ-%d", i)
		    result := producer.SendMessageSync(&rocketmq.Message{Topic: "test", Body: msg})
		    fmt.Println(fmt.Sprintf("send message: %s result: %s", msg, result))
        }
        time.Sleep(10 * time.Second)
	    producer.Shutdown()
    }
    ```
- Send ordered message
    ```go
    type queueSelectorByOrderID struct{}

    func (s queueSelectorByOrderID) Select(size int, m *rocketmq.Message, arg interface{}) int{
       return arg.(int) % size
    }
    func SendOrderMessge(producer rocketmq.producer, topic, body string, order int){
        selector := queueSelectorByOrderID{}
        r := producer.SendMessageOrderly(
            &rocketmq.Message{Topic: topic, Body: body}, selector, arg, 3)
        fmt.Printf("send result: %v", r)
    }
    ```
- Push Consumer
    ```go
    func PushConsumeMessage() {
	    fmt.Println("Start Receiving Messages...")
	    consumer, _ := rocketmq.NewPushConsumer(&rocketmq.ConsumerConfig{
            GroupID: "testGroupId", 
            NameServer: "localhost:9876",
            ConsumerThreadCount: 2, 
            MessageBatchMaxSize: 16})
	    // MUST subscribe topic before consumer started.
	    consumer.Subscribe("test", "*", func(msg    *rocketmq.MessageExt)rocketmq.ConsumeStatus {
		    fmt.Printf("A message received: \"%s\" \n", msg.Body)
		    return rocketmq.ConsumeSuccess})
	    consumer.Start()
	    defer consumer.Shutdown()
	    fmt.Printf("consumer: %s started...\n", consumer)
	    time.Sleep(10 * time.Minute)
    }
    ```
- Pull Consumer
    ```go
    func Pull(consumer *rocketmq.DefaultPullConsumer, topic, expression string, maxNum int){
        mqs := consumer.FetchSubscriptionMessageQueues(topic)
        offsets := make(map[string]int64)
        PULL:
            for {
                for _, mq := range mqs {
                    pr := consumer.Pull(mq, expression, offsets[mq.ID], maxNum)
                    switch pr.Status {
                    case rocketmq.PullNoNewMsg:
				        break PULL
			        case rocketmq.PullFound:
				        fallthrough
			        case rocketmq.PullNoMatchedMsg:
				        fallthrough
			        case rocketmq.PullOffsetIllegal:
				        offsets[mq.ID] = pr.NextBeginOffset
			        case rocketmq.PullBrokerTimeout:
				        fmt.Println("broker timeout occur")
			        }
                }
            }
    }
    ```
- [Full example](../examples)