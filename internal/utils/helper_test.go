package utils

import (
	"bytes"
	"compress/zlib"
	"testing"
)

func Test_UnCompress(t *testing.T) {
	var b bytes.Buffer
	var ori_s string = "hello, go"
	zr := zlib.NewWriter(&b)
	zr.Write([]byte(ori_s))
	zr.Close()

	ret_bytes := UnCompress(b.Bytes())
	if string(ret_bytes) != ori_s {
		t.Errorf("UnCompress was incorrect, got %s, want: %s .", ret_bytes, []byte(ori_s))
	}
}
