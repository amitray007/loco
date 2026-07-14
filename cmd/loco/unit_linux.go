//go:build linux

package main

import (
	"os"
	"strings"
)

// serviceUnitExecutable extracts the executable from a systemd unit's ExecStart line so
// doctor can verify it still exists. Returns "" if not locatable.
func serviceUnitExecutable(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "ExecStart=") {
			continue
		}
		value := strings.TrimPrefix(line, "ExecStart=")
		value = strings.TrimSpace(value)
		if value == "" {
			return ""
		}
		// The executable is the first token. It may be double-quoted (systemdQuote wraps
		// paths containing spaces).
		if strings.HasPrefix(value, `"`) {
			if end := strings.Index(value[1:], `"`); end >= 0 {
				return strings.ReplaceAll(value[1:1+end], `\"`, `"`)
			}
			return ""
		}
		return strings.Fields(value)[0]
	}
	return ""
}
