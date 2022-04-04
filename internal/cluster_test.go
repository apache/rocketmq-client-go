package internal

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
	"testing"
)

func TestParseBrokerAddrs(t *testing.T) {
	str := `{0: "127.0.0.1:1111",1: "127.0.0.1:1112"}`
	blob := []byte(str)
	var yamlmap map[int]string
	yaml.Unmarshal(blob, &yamlmap)
	for k, v := range yamlmap {
		fmt.Println(fmt.Sprintf("k=%d, v=%s", k, v))
	}

}

func TestJsonToMap(t *testing.T) {
	var jsonString = []byte(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:"127.0.0.1:10911"},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`)
	cluster, err := ParseClusterInfo(string(jsonString))

	var cl = &ClusterInfo{}
	yaml.Unmarshal(jsonString, cl)

	assert.True(t, err == nil)
	fmt.Println(cluster)
}