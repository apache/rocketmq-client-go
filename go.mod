module github.com/apache/rocketmq-client-go/v2

require (
	github.com/emirpasic/gods v1.12.0
	github.com/golang/mock v1.3.1
	github.com/json-iterator/go v1.1.9
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.4.1
	github.com/smartystreets/goconvey v0.0.0-20190710185942-9d28bd7c0945
	github.com/stretchr/testify v1.3.0
	github.com/tidwall/gjson v1.2.1
	github.com/tidwall/match v1.0.1 // indirect
	github.com/tidwall/pretty v0.0.0-20190325153808-1166b9ac2b65 // indirect
	go.uber.org/atomic v1.5.1
	stathat.com/c/consistent v1.0.0
)

replace stathat.com/c/consistent v1.0.0 => github.com/stathat/consistent v1.0.0

go 1.13
