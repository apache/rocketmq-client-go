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
package client_test

import "fmt"
import "testing"
import "../client"

func TestCreateMessage(test *testing.T){
    fmt.Println("-----TestCreateMessage Start----")
    client.CreateMessage("testTopic")
    fmt.Println("-----TestCreateMessage Finish----")
}
func TestDestroyMessage(test *testing.T){
    fmt.Println("-----TestCreateMessage Start----")
    msg := client.CreateMessage("testTopic")
    client.DestroyMessage(msg)
    fmt.Println("-----TestCreateMessage Finish----")
}
func TestSetMessageKeys(test *testing.T){
    fmt.Println("-----TestSetMessageKeys Start----")
    msg := client.CreateMessage("testTopic")
    len := client.SetMessageKeys(msg,"testKey")
    fmt.Println("Len:",len)
    client.DestroyMessage(msg)
    fmt.Println("-----TestCreateMessage Finish----")
}
func TestCreateProducer(test *testing.T){
    fmt.Println("-----TestCreateProducer Start----")
    client.CreateProducer("testGroupId")
    fmt.Println("-----TestCreateProducer Finish----")
}
