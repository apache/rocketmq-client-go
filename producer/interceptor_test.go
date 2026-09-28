package producer

import (
	"testing"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/stretchr/testify/assert"
)

// mockTraceDispatcher is a simple implementation of TraceDispatcher interface
type mockTraceDispatcher struct{}

func (m *mockTraceDispatcher) GetTraceTopicName() string {
	return "TRACE_TOPIC"
}

func (m *mockTraceDispatcher) Start() {
	// Mock implementation
}

func (m *mockTraceDispatcher) Append(ctx internal.TraceContext) bool {
	// Mock implementation
	return true
}

func (m *mockTraceDispatcher) Close() {
	// Mock implementation
}

func TestNewTraceInterceptor_WithTypedNil(t *testing.T) {
	var typedNil *mockTraceDispatcher = nil
	var interfaceDispatcher internal.TraceDispatcher = typedNil

	assert.True(t, interfaceDispatcher != nil, "Typed nil interface should not be nil")

	assert.NotPanics(t, func() {
		interceptor := newTraceInterceptor(interfaceDispatcher)
		assert.NotNil(t, interceptor)
	}, "newTraceInterceptor should safely handle typed nil dispatcher without panic")
}

func TestNewTraceInterceptor_WithNil(t *testing.T) {
	assert.NotPanics(t, func() {
		interceptor := newTraceInterceptor(nil)
		assert.NotNil(t, interceptor)
	})
}
