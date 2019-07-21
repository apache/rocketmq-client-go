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

var (
	CompressedFlag = 0x1

	MultiTagsFlag = 0x1 << 1

	TransactionNotType = 0

	TransactionPreparedType = 0x1 << 2

	TransactionCommitType = 0x2 << 2

	TransactionRollbackType = 0x3 << 2
)

func GetTransactionValue(flag int) int {
	return flag & TransactionRollbackType
}

func ResetTransactionValue(flag int, typeFlag int) int {
	return (flag & (^TransactionRollbackType)) | typeFlag
}

func ClearCompressedFlag(flag int) int {
	return flag & (^CompressedFlag)
}
