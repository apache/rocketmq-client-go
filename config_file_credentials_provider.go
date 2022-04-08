package rocketmq

type ConfigFileCredentialsProvider struct {
	accessKey    string
	accessSecret string
	valid        bool
}

//TODO ConfigFileCredentialsProvider
func NewConfigFileCredentialsProvider() *ConfigFileCredentialsProvider {
	c := &ConfigFileCredentialsProvider{
		valid: false,
	}
	return c
}

func (c *ConfigFileCredentialsProvider) GetCredentials() *Credentials {
	if c == nil || !c.valid {
		return nil
	}
	return NewCredentialsV1(c.accessKey, c.accessSecret)
}
