package utils

import "strings"

const namespaceSeparator = "%"
const retryPrefix = "%RETRY%"
const dlqPrefix = "%DLQ%"

func WrapNamespace(namespace, resourceWithOutNamespace string) string {
	if IsEmpty(namespace) || IsEmpty(resourceWithOutNamespace) {
		return resourceWithOutNamespace
	}

	if isAlreadyWithNamespace(resourceWithOutNamespace, namespace) {
		return resourceWithOutNamespace
	}

	return namespace + namespaceSeparator + resourceWithOutNamespace
}

func isAlreadyWithNamespace(resource, namespace string) bool {
	if IsEmpty(namespace) || IsEmpty(resource) {
		return false
	}
	return strings.Contains(resource, namespace+namespaceSeparator)
}

func WithoutNamespace(resource string) string {
	if len(resource) == 0 {
		return resource
	}
	resourceWithoutNamespace := ""
	if strings.HasPrefix(resource, retryPrefix) {
		resourceWithoutNamespace += retryPrefix
	} else if strings.HasPrefix(resource, dlqPrefix) {
		resourceWithoutNamespace += dlqPrefix
	}
	index := strings.LastIndex(resource, namespaceSeparator)
	if index > 0 {
		resourceWithoutNamespace += resource[index+1:]
	} else {
		resourceWithoutNamespace = resource
	}
	return resourceWithoutNamespace
}
