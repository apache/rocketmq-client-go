package utils

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet_NormalUsage(t *testing.T) {
	set := NewSet()
	set.AddKV("name", "xiaoming")
	set.AddKV("age", "18")
	set.Add(StringUnique("hello"))

	name, ok := set.Contains("name")
	assert.True(t, ok)
	assert.Equal(t, "xiaoming", name.UniqueID())

	_, ok = set.Contains("gender")
	assert.False(t, ok)

	assert.Equal(t, 3, set.Len())
}

func TestSet_MarshalJSON(t *testing.T) {
	st := NewSet()

	st.AddKV("name", "xiaoming")
	st.AddKV("age", "18")
	st.Add(StringUnique("hello"))

	data, err := json.Marshal(&st)
	assert.Nil(t, err)
	t.Log(string(data))
}
