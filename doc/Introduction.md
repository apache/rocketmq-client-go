
----------
## RocketMQ Client Go

### 1. Go Version
* go1.11.1 darwin/amd64

### 2. Dependency
* [librocketmq](https://github.com/apache/rocketmq-client-cpp)	

### 3. Build and Install
#### macOS Platform (macOS Mojave 10.14)
* Install Compile tools (homebrew package manager)
    ```
    1. xcode-select --install
    3. brew install cmake
    4. brew install automake
    ```
* Install dependencies
    1. [Go official download](https://golang.org/dl/)

    2. Get go client package
    ```
    go get github.com/gaufung/rocketmq-client-go
    ```
    3. [librocketmq](https://github.com/apache/rocketmq-client-cpp)
        - `git clone https://github.com/apache/rocketmq-client-cpp`
        - `cd rocketmq-client-cpp`
        - `sudo sh build.sh` 
        - `cp bin/librocketmq.dylib /usr/local/lib`
        - `sudo mkdir /usr/local/include/rocketmq`
        - `sudo cp incldue/* /usr/local/incldue/rocketmq/`

#### Linux


#### Windows

----------
## How to use

- import package
    ```
    import "github.com/apache/rocketmq-client-go/core"
    ```
- Send message
    ```go
    func SendMessagge(){
        producer := rocketmq.NewProduer(&rocketmq.ProducerConfig{GroupID: "testGroup", NameServer: "localhost:9876"})
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
- [Full example](../examples)