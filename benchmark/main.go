package main

import (
	"fmt"
	"os"
	"strings"
)

type command interface {
	usage()
	run(args []string)
}

var (
	cmds        = map[string]command{}
	longText    = ""
	longTextLen = 0
)

func init() {
	longText = strings.Repeat("0123456789", 100)
	longTextLen = len(longText)
}

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
	println(os.Args[0] + " commandName [...]")
	for _, cmd := range cmds {
		cmd.usage()
	}
}

// go run *.go [command name] [command args]
func main() {
	if len(os.Args) < 2 {
		println("error:lack cmd name\n")
		usage()
		return
	}

	name := os.Args[1]
	cmd, ok := cmds[name]
	if !ok {
		fmt.Printf("command %s is not supported\n", name)
		usage()
		return
	}

	cmd.run(os.Args[2:])
}
