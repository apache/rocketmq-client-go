package rocketmq

type StaticCredentialsProvider struct {
	accessKey    string
	accessSecret string
}

func NewStaticCredentialsProvider(accessKey string, accessSecret string) *StaticCredentialsProvider {
	c := &StaticCredentialsProvider{
		accessKey:    accessKey,
		accessSecret: accessSecret,
	}
	return c
}

func (c *StaticCredentialsProvider) GetCredentials() *Credentials {
	if c == nil {
		return nil
	}
	return NewCredentialsV1(c.accessKey, c.accessSecret)
}
