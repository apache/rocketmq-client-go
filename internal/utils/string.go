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

package utils

import (
	"fmt"
)

// HashString hashes a string to a unique hashcode.
func HashString(s string) int {
	val := []byte(s)
	var h int32

	for idx := range val {
		h = 31*h + int32(val[idx])
	}

	return int(h)
}

func StrJoin(str, key string, value interface{}) string {
	if key == "" || value == "" {
		return str
	}

	return str + key + ": " + fmt.Sprint(value) + ", "
}
