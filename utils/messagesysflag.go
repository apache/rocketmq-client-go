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



var(
	COMPRESSED_FLAG = 0x1

	MULTI_TAGS_FLAG = 0x1 << 1

	TRANSACTION_NOT_TYPE = 0

	TRANSACTION_PREPARED_TYPE = 0x1 << 2

	TRANSACTION_COMMIT_TYPE = 0x2 << 2

	TRANSACTION_ROLLBACK_TYPE = 0x3 << 2
)

func GetTransactionValue(flag int) int {
	return flag & TRANSACTION_ROLLBACK_TYPE
}

func ResetTransactionValue(flag int, typeFlag int) int {
	return (flag & (^TRANSACTION_ROLLBACK_TYPE)) | typeFlag
}

func ClearCompressedFlag(flag int) int {
	return flag & (^COMPRESSED_FLAG)
}