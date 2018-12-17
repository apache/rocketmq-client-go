package rocketmq

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLogConfig_String(t *testing.T) {
	logc := LogConfig{Path: "/log/path1", FileNum: 3, FileSize: 1 << 20, Level: LogLevelDebug}
	assert.Equal(t, "{Path:/log/path1 FileNum:3 FileSize:1048576 Level:Debug}", logc.String())
	logc.Level = LogLevelFatal
	assert.Equal(t, "{Path:/log/path1 FileNum:3 FileSize:1048576 Level:Fatal}", logc.String())
	logc.Level = LogLevelError
	assert.Equal(t, "{Path:/log/path1 FileNum:3 FileSize:1048576 Level:Error}", logc.String())
	logc.Level = LogLevelWarn
	assert.Equal(t, "{Path:/log/path1 FileNum:3 FileSize:1048576 Level:Warn}", logc.String())
	logc.Level = LogLevelInfo
	assert.Equal(t, "{Path:/log/path1 FileNum:3 FileSize:1048576 Level:Info}", logc.String())
	logc.Level = LogLevelTrace
	assert.Equal(t, "{Path:/log/path1 FileNum:3 FileSize:1048576 Level:Trace}", logc.String())
	logc.Level = LogLevelError
}
