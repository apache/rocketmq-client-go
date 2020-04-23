## RocketMQ Client Go 
[![Build Status](https://travis-ci.org/apache/rocketmq-client-go.svg?branch=native)](https://travis-ci.org/apache/rocketmq-client-go) 
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Go Report Card](https://goreportcard.com/badge/github.com/apache/rocketmq-client-go)](https://goreportcard.com/report/github.com/apache/rocketmq-client-go)
[![GoDoc](https://img.shields.io/badge/Godoc-reference-blue.svg)](https://godoc.org/github.com/apache/rocketmq-client-go)
[![CodeCov](https://codecov.io/gh/apache/rocketmq-client-go/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/rocketmq-client-go)
[![GitHub release](https://img.shields.io/badge/release-download-default.svg)](https://github.com/apache/rocketmq-client-go/releases)
[![Average time to resolve an issue](http://isitmaintained.com/badge/resolution/apache/rocketmq-client-go.svg)](http://isitmaintained.com/project/apache/rocketmq-client-go "Average time to resolve an issue")
[![Percentage of issues still open](http://isitmaintained.com/badge/open/apache/rocketmq-client-go.svg)](http://isitmaintained.com/project/apache/rocketmq-client-go "Percentage of issues still open")
![Twitter Follow](https://img.shields.io/twitter/follow/ApacheRocketMQ?style=social)

(The Apache RocketMQ Client in Pure Go has been released, Welcome have a try on [the native version](https://github.com/apache/rocketmq-client-go/tree/native))

* The client is using cgo to call [rocketmq-client-cpp](https://github.com/apache/rocketmq-client-cpp), which has been proven robust and widely adopted within Alibaba Group by many business units for more than three years.
----------
## [Due Diligence](https://github.com/apache/rocketmq-client-go/issues/423)
[Here](https://github.com/apache/rocketmq-client-go/issues/423), we sincerely invite you to take a minute to feedback on your usage scenario. 
[Click Here](https://github.com/apache/rocketmq-client-go/issues/423) or go to [ISSUE #423](https://github.com/apache/rocketmq-client-go/issues/423) if you accept.

----------
## Features
At present, this SDK supports
* sending message in synchronous mode
* sending message in orderly mode
* sending message in oneway mode
* sending transaction message
* consuming message using push model
* consuming message using pull model(depends cpp core)

----------
## How to use
* Step-by-step instruction are provided in [RocketMQ Go Client Introduction](./doc/Introduction.md)
* Consult [RocketMQ Quick Start](https://rocketmq.apache.org/docs/quick-start/) to setup rocketmq broker and nameserver.

----------
## Apache RocketMQ Community
* [RocketMQ Community Projects](https://github.com/apache/rocketmq-externals)

----------
## Contact us
* Mailing Lists: <https://rocketmq.apache.org/about/contact/>
* Home: <https://rocketmq.apache.org>
* Docs: <https://rocketmq.apache.org/docs/quick-start/>
* Issues: <https://github.com/apache/rocketmq-client-go/issues>
* Ask: <https://stackoverflow.com/questions/tagged/rocketmq>
* Slack: <https://rocketmq-community.slack.com/>
 
---------- 
## How to Contribute
  Contributions are warmly welcome! Be it trivial cleanup, major new feature or other suggestion. Read this [how to contribute](http://rocketmq.apache.org/docs/how-to-contribute/) guide for more details. 
   
   
----------
## License
  [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation
