package logger

import (
	"os"

	"github.com/prometheus/common/log"
)

func NewWithKeys(keys map[string]string) log.Logger {
	l := log.NewLogger(os.Stderr)
	for key, value := range keys {
		l = l.With(key, value)
	}
	return l
}