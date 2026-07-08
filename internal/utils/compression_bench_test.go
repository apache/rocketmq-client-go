package utils

import (
	"bytes"
	"compress/zlib"
	"io/ioutil"
	"math/rand"
	"strconv"
	"testing"
)

func generateTestData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

func compressTestData(data []byte) []byte {
	var buf bytes.Buffer
	writer, _ := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	writer.Write(data)
	writer.Close()
	return buf.Bytes()
}

func UnCompressOriginal(data []byte) []byte {
	rdata := bytes.NewReader(data)
	r, err := zlib.NewReader(rdata)
	if err != nil {
		return data
	}
	defer r.Close()
	retData, err := ioutil.ReadAll(r)
	if err != nil {
		return data
	}
	return retData
}

var testDataSizes = []int{1024, 64 * 1024, 512 * 1024, 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024}

func BenchmarkUnCompress(b *testing.B) {
	for _, size := range testDataSizes {
		data := generateTestData(size)
		compressed := compressTestData(data)

		b.Run("New_"+formatSize(size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result := UnCompress(compressed)
				_ = result
			}
		})

		b.Run("Original_"+formatSize(size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result := UnCompressOriginal(compressed)
				_ = result
			}
		})
	}
}

func BenchmarkMemoryUsage(b *testing.B) {
	// 测试大内存使用情况
	largeData := generateTestData(4 * 1024 * 1024) // 4MB
	compressed := compressTestData(largeData)

	b.Run("New_Memory", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result := UnCompress(compressed)
			_ = result
		}
	})

	b.Run("Original_Memory", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result := UnCompressOriginal(compressed)
			_ = result
		}
	})
}

func formatSize(bytes int) string {
	if bytes < 1024 {
		return strconv.Itoa(bytes) + "B"
	} else if bytes < 1024*1024 {
		return strconv.Itoa(bytes/1024) + "KB"
	} else if bytes < 1024*1024*1024 {
		return strconv.Itoa(bytes/(1024*1024)) + "MB"
	} else {
		return strconv.Itoa(bytes/(1024*1024*1024)) + "GB"
	}
}
