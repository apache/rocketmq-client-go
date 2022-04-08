package utils

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
)

func HmacSha1(valBytes []byte, keyBytes []byte) string {
	h := hmac.New(sha1.New, keyBytes)
	h.Write(valBytes)
	return hex.EncodeToString(h.Sum(nil))
}
