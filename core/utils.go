package rocketmq

import "fmt"

func strJoin(str, key string, value interface{}) string {
	if key == "" || value == "" {
		return str
	}

	return str + key + ": " + fmt.Sprint(value) + ", "
}
