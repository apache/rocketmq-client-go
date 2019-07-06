package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVerifyIP(t *testing.T) {
	IPs := "127.0.0.1:9876"
	err := VerifyIP(IPs)
	assert.Nil(t, err)

	IPs = "12.24.123.243:10911"
	err = VerifyIP(IPs)
	assert.Nil(t, err)

	IPs = "xa2.0.0.1:9876"
	err = VerifyIP(IPs)
	assert.Equal(t, "IP addr error", err.Error())

	IPs = "333.0.0.1:9876"
	err = VerifyIP(IPs)
	assert.Equal(t, "IP addr error", err.Error())

	IPs = "127.0.0.1:9876;12.24.123.243:10911"
	err = VerifyIP(IPs)
	assert.Equal(t, "multiple IP addr does not support", err.Error())
}
