package utils

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var _testFuncMap = map[string]func(string) string{
	"RectifyJsonIntKeys":       RectifyJsonIntKeys,
	"RectifyJsonIntKeysByChar": RectifyJsonIntKeysByChar,
}

var _brokerClusterInfoRespJsonTestData = []string{
	// test normal response with two broker
	`{
    "brokerAddrTable": {
        "broker-002": {
            "brokerAddrs": {
                0: "192.168.1.102:10911"
            },
            "brokerName": "broker-002",
            "cluster": "DefaultCluster",
            "enableActingMaster": false
        },
        "broker-001": {
            "brokerAddrs": {
                0: "192.168.1.101:10911"
            },
            "brokerName": "broker-001",
            "cluster": "DefaultCluster",
            "enableActingMaster": false
        }
    },
    "clusterAddrTable": {
        "DefaultCluster": [
            "broker-002",
            "broker-001"
        ]
    }
}`, // test normal response with one broker
	`{"brokerAddrTable":{"broker_37_master":{"brokerAddrs":{0:"172.16.16.37:10911"},"brokerName":"broker_37_master","cluster":"37_cluster"}},"clusterAddrTable":{"37_cluster":["broker_37_master"]}}`,
}

func TestRectifyJsonIntKeys(t *testing.T) {
	testCases := []struct {
		name     string
		jsonStr  string
		needConv bool // 是否需要纠正
	}{
		{
			// OK: 正常的 JSON
			name:     "ok: Properly quoted keys",
			jsonStr:  `{"key": {"anotherKey": "value"}}`,
			needConv: false,
		},
		{
			// OK: JSON String 包含整数键类型
			name:     "ng: Integer key with escaped quote inside string",
			jsonStr:  `{"key": "value with \" and", "value": "{1: \"integer key\"}"}`,
			needConv: false,
		},
		{
			// OK: String 里面包含转义的双引号
			name:     "ok: Escaped quotes inside string",
			jsonStr:  `{"key": "value with escaped quote: \" here"}`,
			needConv: false,
		},
		{
			// NG: 类型为 Int 的键放在嵌套的对象中
			name:     "ng: Integer key inside nested object",
			jsonStr:  `{"key": {1: "value"}}`,
			needConv: true,
		},
		{
			// OK: Inter Key 在字符串中
			name:     "ok: Integer key inside string",
			jsonStr:  `{"key": "value containing 1: within"}`,
			needConv: false,
		},
		{
			// NG: JSON 包含转义的双引号，且包含整数键类型
			name:     "ng: Integer key with escaped quote inside string",
			jsonStr:  `{"key": "value with \" and", 1: "integer key"}`,
			needConv: true,
		},
		{ // NG: JSON 包含超长的跨多行的 String
			name: "ng: String include very long text and Integer key",
			jsonStr: `{
    "long_nested_result": "{\"brokerAddrTable\": {\"broker_37_master\": {\"brokerAddrs\": {\"0\": \"172.16.16.37:10911\"}, \"brokerName\": \"broker_37_master\", \"cluster\": \"37_cluster\"}}, \"clusterAddrTable\": {\"37_cluster\": [\"broker_37_master\"]}}",
    12345: 12345
}`,
			needConv: true,
		},
		{
			// OK: JSON 包含整数数组
			name:     "ok: Integer value in array",
			jsonStr:  `{"key": [1, 2, 3, 4, 5]}`,
			needConv: false,
		},
		{
			// NG: JSON 包含整数数组
			name:     "ok: Integer value in array",
			jsonStr:  `{6: [1, 2, 3, 4, 5]}`,
			needConv: true,
		},
		{
			// OK: JSON 多层
			name:     "ok: JSON nested",
			jsonStr:  `{"foo": {"bar": [1, 2, 3, 4, 5]}}`,
			needConv: false,
		},
		{
			// NG: JSON 多层
			name:     "ok: JSON nested",
			jsonStr:  `{"foo": {6: [1, 2, 3, 4, 5]}}`,
			needConv: true,
		},
	}

	for _, testCase := range testCases {
		var preLoadResult map[string]interface{}
		err := json.Unmarshal([]byte(testCase.jsonStr), &preLoadResult)
		if testCase.needConv { // 需要转换才能加载，否则直接加载会报错
			assert.NotNil(t, err, "json is invalid, need convert")
		} else { // 无需转换，就能直接加载
			assert.Nil(t, err, "json is valid, not need convert")
		}

		for funcName, funcConv := range _testFuncMap {
			correctedJSON := funcConv(testCase.jsonStr)
			// Token 级别的修正函数不需要测试，因为它会把 Token 之间的空白字符剔除掉
			// 专门用于测试字符级别的修正函数，检查是否前后一致，对于不需要修正的字符串，操作前后应相同
			if funcName == "RectifyJsonIntKeysByChar" && !testCase.needConv {
				if correctedJSON != testCase.jsonStr {
					fmt.Printf("Original  JSON: %s\n", testCase.jsonStr)
					fmt.Printf("Corrected JSON: %s\n\n", correctedJSON)
				}
				assert.True(t, correctedJSON == testCase.jsonStr, "not need convert, but json is not equal after process")
			}

			var result map[string]interface{}
			err2 := json.Unmarshal([]byte(correctedJSON), &result)
			if err2 != nil {
				fmt.Printf("Func %s Test %s failed. err %v\n", funcName, testCase.name, err2)
				fmt.Printf("Original  JSON: %s\n", testCase.jsonStr)
				fmt.Printf("Corrected JSON: %s\n\n", correctedJSON)
				t.FailNow()
			}
			fmt.Printf("%s Test %s passed.\n\n", funcName, testCase.name)
		}
	}
}

func TestRectifyIntKeysInArray(t *testing.T) {
	testCases := []struct {
		name     string
		jsonStr  string
		needConv bool
	}{
		{
			// OK: JSON in Array
			name:     "ok: object in array",
			jsonStr:  `[{"1": "val"}]`,
			needConv: false,
		},
		{
			// NG: JSON in Array
			name:     "ng: object in array",
			jsonStr:  `[{"1": "val"}, {2: "val"}]`,
			needConv: true,
		},
		{
			// OK: JSON in Array(more deep)
			name:     "ok: object in array(more deep)",
			jsonStr:  `[{"1": "val"}, {"2": [{"3": [1, 2, 3]}]}]`,
			needConv: false,
		},
		{
			// NG: JSON in Array(more deep)
			name:     "ng: object in array(more deep)",
			jsonStr:  `[{"1": "val"}, {"2": [{3: [1, 2, 3]}]}]`,
			needConv: true,
		},
	}
	for _, testCase := range testCases {
		var preLoadResult []map[string]interface{}
		err := json.Unmarshal([]byte(testCase.jsonStr), &preLoadResult)
		if testCase.needConv { // 需要转换才能加载，否则直接加载会报错
			assert.NotNil(t, err, "json is invalid, need convert")
		} else { // 无需转换，就能直接加载
			assert.Nil(t, err, "json is valid, not need convert")
		}

		for funcName, funcConv := range _testFuncMap {
			correctedJSON := funcConv(testCase.jsonStr)
			if funcName == "RectifyJsonIntKeysByChar" && !testCase.needConv {
				assert.True(t, correctedJSON == testCase.jsonStr, "not need convert, but json is not equal after process")
			}

			var result []map[string]interface{}
			err2 := json.Unmarshal([]byte(correctedJSON), &result)
			if err2 != nil {
				fmt.Printf("Func %s Test %s failed. err %v\n", funcName, testCase.name, err2)
				fmt.Printf("Original  JSON: %s\n", testCase.jsonStr)
				fmt.Printf("Corrected JSON: %s\n\n", correctedJSON)
				t.FailNow()
			}
			fmt.Printf("%s Test %s passed.\n\n", funcName, testCase.name)
		}
	}

}
func TestRectifyJsonIntKeysWithSingleInt(t *testing.T) {
	testCases := []struct {
		name     string
		jsonStr  string
		needConv bool
	}{
		{
			// OK: JSON 单个整数
			name:     "ok: single int",
			jsonStr:  `1`,
			needConv: false,
		},
		{
			// OK: JSON 单个大整数
			name:     "ok: large single int",
			jsonStr:  `999999999`,
			needConv: false,
		},
	}

	for _, testCase := range testCases {
		var preLoadResult int
		err := json.Unmarshal([]byte(testCase.jsonStr), &preLoadResult)
		if testCase.needConv { // 需要转换才能加载，否则直接加载会报错
			assert.NotNil(t, err, "json is invalid, need convert")
		} else { // 无需转换，就能直接加载
			assert.Nil(t, err, "json is valid, not need convert")
		}

		for funcName, funcConv := range _testFuncMap {
			correctedJSON := funcConv(testCase.jsonStr)
			if funcName == "RectifyJsonIntKeysByChar" && !testCase.needConv {
				assert.True(t, correctedJSON == testCase.jsonStr, "not need convert, but json is not equal after process")
			}

			var result int
			err2 := json.Unmarshal([]byte(correctedJSON), &result)
			if err2 != nil {
				fmt.Printf("Func %s Test %s failed. err %v\n", funcName, testCase.name, err2)
				fmt.Printf("Original  JSON: %s\n", testCase.jsonStr)
				fmt.Printf("Corrected JSON: %s\n\n", correctedJSON)
				t.FailNow()
			}
			fmt.Printf("%s Test %s passed.\n\n", funcName, testCase.name)
		}
	}
}
func TestRectifyJsonIntKeysWithArray(t *testing.T) {
	testCases := []struct {
		name     string
		jsonStr  string
		needConv bool
	}{
		{
			// OK: one number in array
			name:     "ok: number array",
			jsonStr:  `[1]`,
			needConv: false,
		},
		{
			// OK: one large number in array
			name:     "ok: large int in array",
			jsonStr:  `[999999999]`,
			needConv: false,
		},
		{
			// OK: multiple number in array
			name:     "ok: multiple large int in array",
			jsonStr:  `[777777777, 888888888, 999999999]`,
			needConv: false,
		},
	}

	for _, testCase := range testCases {
		var preLoadResult []int
		err := json.Unmarshal([]byte(testCase.jsonStr), &preLoadResult)
		if testCase.needConv { // 需要转换才能加载，否则直接加载会报错
			assert.NotNil(t, err, "json is invalid, need convert")
		} else { // 无需转换，就能直接加载
			assert.Nil(t, err, "json is valid, not need convert")
		}

		for funcName, funcConv := range _testFuncMap {
			correctedJSON := funcConv(testCase.jsonStr)
			if funcName == "RectifyJsonIntKeysByChar" && !testCase.needConv {
				assert.True(t, correctedJSON == testCase.jsonStr, "not need convert, but json is not equal after process")
			}

			var result []int
			err2 := json.Unmarshal([]byte(correctedJSON), &result)
			if err2 != nil {
				fmt.Printf("Func %s Test %s failed. err %v\n", funcName, testCase.name, err2)
				fmt.Printf("Original  JSON: %s\n", testCase.jsonStr)
				fmt.Printf("Corrected JSON: %s\n\n", correctedJSON)
				t.FailNow()
			}
			fmt.Printf("%s Test %s passed.\n\n", funcName, testCase.name)
		}
	}
}

func TestRectifyJsonIntKeysWithBrokerClusterInfoResp(t *testing.T) {
	for _, jsonData := range _brokerClusterInfoRespJsonTestData {
		for funcName, funcConv := range _testFuncMap {
			correctedJSON := funcConv(jsonData)

			var result map[string]interface{}
			err := json.Unmarshal([]byte(correctedJSON), &result)
			assert.Nil(t, err, "%s failed to rectify int key JSON", funcName)

			prettyJSON, err2 := json.MarshalIndent(result, "", "    ")
			assert.Nil(t, err2, "%s failed to marshal int key JSON", funcName)
			fmt.Printf("%s converted %s\n", funcName, string(prettyJSON))
		}
	}
}

func BenchmarkRectifyJsonIntKeys(b *testing.B) {
	for n := 0; n < b.N; n++ {
		for _, jsonData := range _brokerClusterInfoRespJsonTestData {
			RectifyJsonIntKeys(jsonData)
		}
	}
}

func BenchmarkRectifyJsonIntKeysByChar(b *testing.B) {
	for n := 0; n < b.N; n++ {
		for _, jsonData := range _brokerClusterInfoRespJsonTestData {
			RectifyJsonIntKeysByChar(jsonData)
		}
	}
}
