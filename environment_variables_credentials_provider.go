package rocketmq

import "os"

const EnvironmentAccessKey = "ROCKETMQ_ACCESS_KEY"
const EnvironmentAccessSecret = "ROCKETMQ_ACCESS_SECRET"

type EnvironmentVariablesCredentialsProvider struct {
	accessKey    string
	accessSecret string
}

func NewEnvironmentVariablesCredentialsProvider() *EnvironmentVariablesCredentialsProvider {
	c := &EnvironmentVariablesCredentialsProvider{
		accessKey:    os.Getenv(EnvironmentAccessKey),
		accessSecret: os.Getenv(EnvironmentAccessSecret),
	}
	return c
}

func (c *EnvironmentVariablesCredentialsProvider) GetCredentials() *Credentials {
	return NewCredentialsV1(c.accessKey, c.accessSecret)
}
