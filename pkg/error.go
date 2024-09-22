package pkg

import (
	"strings"
)

func IsDBDuplicateError(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), " duplicate ")
}