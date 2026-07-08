package consumer

import (
	"reflect"
	"testing"
	"time"
)

func getFieldString(obj interface{}, field string) string {
	v := reflect.Indirect(reflect.ValueOf(obj))
	return v.FieldByNameFunc(func(n string) bool {
		return n == field
	}).String()
}

func TestWithRemotingTimeout(t *testing.T) {
	opt := defaultPushConsumerOptions()
	WithRemotingTimeout(3*time.Second, 4*time.Second, 5*time.Second)(&opt)
	if timeout := opt.RemotingClientConfig.ConnectionTimeout; timeout != 3*time.Second {
		t.Errorf("consumer option WithRemotingTimeout connectionTimeout. want:%s, got=%s", 3*time.Second, timeout)
	}
	if timeout := opt.RemotingClientConfig.ReadTimeout; timeout != 4*time.Second {
		t.Errorf("consumer option WithRemotingTimeout readTimeout. want:%s, got=%s", 4*time.Second, timeout)
	}
	if timeout := opt.RemotingClientConfig.WriteTimeout; timeout != 5*time.Second {
		t.Errorf("consumer option WithRemotingTimeout writeTimeout. want:%s, got=%s", 5*time.Second, timeout)
	}
}

func TestWithUnitName(t *testing.T) {
	opt := defaultPushConsumerOptions()
	unitName := "unsh"
	WithUnitName(unitName)(&opt)
	if opt.UnitName != unitName {
		t.Errorf("consumer option WithUnitName. want:%s, got=%s", unitName, opt.UnitName)
	}
}

func TestWithNameServerDomain(t *testing.T) {
	opt := defaultPushConsumerOptions()
	nameServerAddr := "http://127.0.0.1:8080/nameserver/addr"
	WithNameServerDomain(nameServerAddr)(&opt)
	domainStr := getFieldString(opt.Resolver, "domain")
	if domainStr != nameServerAddr {
		t.Errorf("consumer option WithUnitName. want:%s, got=%s", nameServerAddr, domainStr)
	}
}

func TestWithNameServerDomainAndUnitName(t *testing.T) {
	unitName := "unsh"
	// test with two different orders
	t.Run("WithNameServerDomain & WithUnitName", func(t *testing.T) {
		addr := "http://127.0.0.1:8080/nameserver/addr"
		opt := defaultPushConsumerOptions()
		WithNameServerDomain(addr)(&opt)
		WithUnitName(unitName)(&opt)

		domainStr := getFieldString(opt.Resolver, "domain")
		expectedAddr := "http://127.0.0.1:8080/nameserver/addr-unsh?nofix=1"
		if domainStr != expectedAddr {
			t.Errorf("consumer option WithNameServerDomain & WithUnitName. want:%s, got=%s", expectedAddr, domainStr)
		}
	})

	t.Run("WithUnitName & WithNameServerDomain", func(t *testing.T) {
		addr := "http://127.0.0.1:8080/nameserver/addr"
		opt := defaultPushConsumerOptions()
		WithUnitName(unitName)(&opt)
		WithNameServerDomain(addr)(&opt)

		domainStr := getFieldString(opt.Resolver, "domain")
		expectedAddr := "http://127.0.0.1:8080/nameserver/addr-unsh?nofix=1"
		if domainStr != expectedAddr {
			t.Errorf("consumer option WithUnitName & WithNameServerDomain. want:%s, got=%s", expectedAddr, domainStr)
		}
	})

	// test with two different orders - name server with query string
	t.Run("WithNameServerDomain & WithUnitName", func(t *testing.T) {
		addr := "http://127.0.0.1:8080/nameserver/addr?labels=abc"
		opt := defaultPushConsumerOptions()
		WithNameServerDomain(addr)(&opt)
		WithUnitName(unitName)(&opt)

		domainStr := getFieldString(opt.Resolver, "domain")
		expectedAddr := "http://127.0.0.1:8080/nameserver/addr-unsh?nofix=1&labels=abc"
		if domainStr != expectedAddr {
			t.Errorf("consumer option WithNameServerDomain & WithUnitName. want:%s, got=%s", expectedAddr, domainStr)
		}
	})

	t.Run("WithUnitName & WithNameServerDomain", func(t *testing.T) {
		addr := "http://127.0.0.1:8080/nameserver/addr?labels=abc"
		opt := defaultPushConsumerOptions()
		WithUnitName(unitName)(&opt)
		WithNameServerDomain(addr)(&opt)

		domainStr := getFieldString(opt.Resolver, "domain")
		expectedAddr := "http://127.0.0.1:8080/nameserver/addr-unsh?nofix=1&labels=abc"
		if domainStr != expectedAddr {
			t.Errorf("consumer option WithUnitName & WithNameServerDomain. want:%s, got=%s", expectedAddr, domainStr)
		}
	})
}
