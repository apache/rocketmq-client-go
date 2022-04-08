package rocketmq

import (
	"time"
)

type Credentials struct {
	accessKey         string
	accessSecret      string
	sessionToken      string
	expirationInstant time.Time
}

type ICredentialsProvider interface {
	GetCredentials() *Credentials
}

func NewCredentialsV1(accessKey string, accessSecret string) *Credentials {
	c := &Credentials{
		accessKey:         accessKey,
		accessSecret:      accessSecret,
		expirationInstant: time.Time{},
	}
	return c
}

func NewCredentialsV2(accessKey string, accessSecret string, sessionToken string, expirationInstant time.Time) *Credentials {
	c := &Credentials{
		accessKey:         accessKey,
		accessSecret:      accessSecret,
		sessionToken:      sessionToken,
		expirationInstant: expirationInstant,
	}
	return c
}

func (c *Credentials) Empty() bool {
	return IsNullOrEmpty(c.accessKey) || IsNullOrEmpty(c.accessSecret)
}

func (c *Credentials) Expired() bool {
	if time.Time.IsZero(c.expirationInstant) {
		return false
	}
	return time.Now().After(c.expirationInstant)
}
