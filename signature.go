package rocketmq

import (
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/utils"
	"google.golang.org/grpc/metadata"
	"time"
)

func sign(clientConfig IClientConfig, metaData metadata.MD) {
	metaData.Append(LANGUAGE_KEY, "DOTNET")
	metaData.Append(CLIENT_VERSION_KEY, "5.0.0")
	if !IsNullOrEmpty(clientConfig.TenantId()) {
		metaData.Append(TENANT_ID_KEY, clientConfig.TenantId())
	}
	if !IsNullOrEmpty(clientConfig.ResourceNamespace()) {
		metaData.Append(NAMESPACE_KEY, clientConfig.ResourceNamespace())
	}
	timeStr := time.Now().Format("20060102T150405Z")
	metaData.Append(DATE_TIME_KEY, timeStr)
	if clientConfig.CredentialsProvider() != nil {
		var credentials = clientConfig.CredentialsProvider().GetCredentials()
		if credentials == nil || credentials.Expired() {
			return
		}
		if IsNullOrEmpty(credentials.sessionToken) {
			metaData.Append(STS_SESSION_TOKEN, credentials.sessionToken)
		}
		if IsNullOrEmpty(credentials.accessKey) || IsNullOrEmpty(credentials.accessSecret) {
			return
		}
		secretData := []byte(credentials.accessSecret)
		data := []byte(timeStr)
		hmac := utils.HmacSha1(data, secretData)
		authorization := fmt.Sprintf("%s %s=%s/%s/%s, %s=%s, %s=%s",
			ALGORITHM_KEY,
			CREDENTIAL_KEY,
			credentials.accessKey,
			clientConfig.Region(),
			clientConfig.ServiceName(),
			SIGNED_HEADERS_KEY,
			DATE_TIME_KEY,
			SIGNATURE_KEY,
			hmac)
		metaData.Append(AUTHORIZATION, authorization)
	}
}
