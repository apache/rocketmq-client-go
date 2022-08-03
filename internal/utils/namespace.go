package utils

import "strings"

const namespaceSeparator = "%"

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
