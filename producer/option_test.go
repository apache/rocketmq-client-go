package producer

import (
	"fmt"
	"reflect"
	"strings"
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
		t.Errorf("consumer option WithUnitName. want:%s, got=%s", unitName, opt.UnitName)
	}
}

func TestWithNameServerDomain(t *testing.T) {
	opt := defaultProducerOptions()
	nameServerAddr := "http://127.0.0.1:8080/nameserver/addr"
	WithNameServerDomain(nameServerAddr)(&opt)
	domainStr := getFieldString(opt.Resolver, "domain")
	if domainStr != nameServerAddr {
		t.Errorf("consumer option WithUnitName. want:%s, got=%s", nameServerAddr, domainStr)
	}
}

func TestWithNameServerDomainAndUnitName(t *testing.T) {
	nameServerAddr := "http://127.0.0.1:8080/nameserver/addr"
	unitName := "unsh"
	suffix := fmt.Sprintf("-%s?nofix=1", unitName)

	// test with two different orders
	t.Run("WithNameServerDomain & WithUnitName", func(t *testing.T) {
		opt := defaultProducerOptions()
		WithNameServerDomain(nameServerAddr)(&opt)
		WithUnitName(unitName)(&opt)

		domainStr := getFieldString(opt.Resolver, "domain")
		if !strings.Contains(domainStr, nameServerAddr) || !strings.Contains(domainStr, suffix) {
			t.Errorf("consumer option should contains %s and %s", nameServerAddr, suffix)
		}
	})

	t.Run("WithUnitName & WithNameServerDomain", func(t *testing.T) {
		opt := defaultProducerOptions()
		WithNameServerDomain(nameServerAddr)(&opt)
		WithUnitName(unitName)(&opt)

		domainStr := getFieldString(opt.Resolver, "domain")
		if !strings.Contains(domainStr, nameServerAddr) || !strings.Contains(domainStr, suffix) {
			t.Errorf("consumer option should contains %s and %s", nameServerAddr, suffix)
		}
	})
}
