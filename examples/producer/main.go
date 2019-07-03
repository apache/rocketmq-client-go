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

package main

import (
	"fmt"
)

type Message struct{
	Topic string
	Body []byte
	switcher bool
}

func doInt(resp interface{}) {
	resp = &Message{
		Topic: "hahha",
		Body: []byte("heiya heiya"),
	}
}

func doStruct(resp *Message) {
	resp = &Message{
		Topic: "hahha",
		Body: []byte("heiya heiya"),
	}
}

func doField(resp *Message) {
	resp.Topic = "haha"
	resp.Body = []byte("lalal")
	resp.switcher = true
}

func main() {
	r := new(Message)
	doInt(r)
	fmt.Printf("after interface, msg: %v\n", r)

	r1 := new(Message)
	doStruct(r1)
	fmt.Printf("after struct, msg: %v\n", r1)

	r2 := new(Message)
	doField(r2)
	fmt.Printf("after field. msg: %v\n", r2)
}



