package rocketmq

//TODO StsCredentialsProvider
type StsCredentialsProvider struct {
	accessKey    string
	accessSecret string
}

func NewStsCredentialsProvider() *StsCredentialsProvider {
	c := &StsCredentialsProvider{}
	return c
}

func (c *StsCredentialsProvider) GetCredentials() *Credentials {
	return NewCredentialsV1(c.accessKey, c.accessSecret)
}
