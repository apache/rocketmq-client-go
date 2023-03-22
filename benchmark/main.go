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

package main

import (
	"fmt"
	"github.com/bilinxing/rocketmq-client-go/v2/rlog"
	"os"
)

type command interface {
	usage()
	run(args []string)
}

var (
	cmds = map[string]command{}
)

func registerCommand(name string, cmd command) {
	if cmd == nil {
		panic("empty command")
	}

	_, ok := cmds[name]
	if ok {
		panic(fmt.Sprintf("%s command existed", name))
	}

	cmds[name] = cmd
}

func usage() {
	rlog.Info("Command", map[string]interface{}{
		"name": os.Args[0],
	})
	for _, cmd := range cmds {
		cmd.usage()
	}
}

// go run *.go [command name] [command args]
func main() {
	if len(os.Args) < 2 {
		rlog.Error("Lack Command Name", nil)
		usage()
		return
	}

	name := os.Args[1]
	cmd, ok := cmds[name]
	if !ok {
		rlog.Error("Command Isn't Supported", map[string]interface{}{
			"command": name,
		})
		usage()
		return
	}

	cmd.run(os.Args[2:])
}
