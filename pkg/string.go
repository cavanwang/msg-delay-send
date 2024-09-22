package pkg

import (
	"encoding/json"
)

func ToJsonBytes(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func FromJsonByte(b []byte, ptr interface{}) {
	_ = json.Unmarshal(b, ptr)
}