package utils

import (
	"bytes"
	"compress/zlib"
	"testing"
)

func TestUnCompress(t *testing.T) {
	var b bytes.Buffer
	var oriStr string = "hello, go"
	zr := zlib.NewWriter(&b)
	zr.Write([]byte(oriStr))
	zr.Close()

	retBytes := UnCompress(b.Bytes())
	if string(retBytes) != oriStr {
		t.Errorf("UnCompress was incorrect, got %s, want: %s .", retBytes, []byte(oriStr))
	}
}
