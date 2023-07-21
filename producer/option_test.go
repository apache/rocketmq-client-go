package producer

import (
	"reflect"
	"testing"
)

func getFieldString(obj interface{}, field string) string {
	v := reflect.Indirect(reflect.ValueOf(obj))
	return v.FieldByNameFunc(func(n string) bool {
		return n == field
	}).String()
}

func TestWithUnitName(t *testing.T) {
	opt := defaultProducerOptions()
	unitName := "unsh"
	WithUnitName(unitName)(&opt)
	if opt.UnitName != unitName {
		t.Errorf("producer option WithUnitName. want:%s, got=%s", unitName, opt.UnitName)
	}
}

func TestWithNameServerDomain(t *testing.T) {
	opt := defaultProducerOptions()
	nameServerAddr := "http://127.0.0.1:8080/nameserver/addr"
	WithNameServerDomain(nameServerAddr)(&opt)
	domainStr := getFieldString(opt.Resolver, "domain")
	if domainStr != nameServerAddr {
		t.Errorf("producer option WithUnitName. want:%s, got=%s", nameServerAddr, domainStr)
	}
}

func TestWithNameServerDomainAndUnitName(t *testing.T) {
	unitName := "unsh"
	// test with two different orders
	t.Run("WithNameServerDomain & WithUnitName", func(t *testing.T) {
		addr := "http://127.0.0.1:8080/nameserver/addr"
		opt := defaultProducerOptions()
		WithNameServerDomain(addr)(&opt)
		WithUnitName(unitName)(&opt)

		domainStr := getFieldString(opt.Resolver, "domain")
		expectedAddr := "http://127.0.0.1:8080/nameserver/addr-unsh?nofix=1"
		if domainStr != expectedAddr {
			t.Errorf("producer option WithNameServerDomain & WithUnitName. want:%s, got=%s", expectedAddr, domainStr)
		}
	})

	t.Run("WithUnitName & WithNameServerDomain", func(t *testing.T) {
		addr := "http://127.0.0.1:8080/nameserver/addr"
		opt := defaultProducerOptions()
		WithUnitName(unitName)(&opt)
		WithNameServerDomain(addr)(&opt)

		domainStr := getFieldString(opt.Resolver, "domain")
		expectedAddr := "http://127.0.0.1:8080/nameserver/addr-unsh?nofix=1"
		if domainStr != expectedAddr {
			t.Errorf("producer option WithUnitName & WithNameServerDomain. want:%s, got=%s", expectedAddr, domainStr)
		}
	})

	// test with two different orders - name server with query string
	t.Run("WithNameServerDomain & WithUnitName", func(t *testing.T) {
		addr := "http://127.0.0.1:8080/nameserver/addr?labels=abc"
		opt := defaultProducerOptions()
		WithNameServerDomain(addr)(&opt)
		WithUnitName(unitName)(&opt)

		domainStr := getFieldString(opt.Resolver, "domain")
		expectedAddr := "http://127.0.0.1:8080/nameserver/addr-unsh?nofix=1&labels=abc"
		if domainStr != expectedAddr {
			t.Errorf("producer option WithNameServerDomain & WithUnitName. want:%s, got=%s", expectedAddr, domainStr)
		}
	})

	t.Run("WithUnitName & WithNameServerDomain", func(t *testing.T) {
		addr := "http://127.0.0.1:8080/nameserver/addr?labels=abc"
		opt := defaultProducerOptions()
		WithUnitName(unitName)(&opt)
		WithNameServerDomain(addr)(&opt)

		domainStr := getFieldString(opt.Resolver, "domain")
		expectedAddr := "http://127.0.0.1:8080/nameserver/addr-unsh?nofix=1&labels=abc"
		if domainStr != expectedAddr {
			t.Errorf("producer option WithUnitName & WithNameServerDomain. want:%s, got=%s", expectedAddr, domainStr)
		}
	})
}
