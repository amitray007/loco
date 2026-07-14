//go:build darwin

package main

import (
	"os"
	"strings"
)

// serviceUnitExecutable extracts ProgramArguments[0] (the executable path) from a
// launchd plist so doctor can verify it still exists. Returns "" if not locatable.
func serviceUnitExecutable(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	text := string(data)
	arrayStart := strings.Index(text, "<array>")
	if arrayStart < 0 {
		return ""
	}
	rest := text[arrayStart:]
	open := strings.Index(rest, "<string>")
	if open < 0 {
		return ""
	}
	rest = rest[open+len("<string>"):]
	end := strings.Index(rest, "</string>")
	if end < 0 {
		return ""
	}
	value := rest[:end]
	value = strings.ReplaceAll(value, "&amp;", "&")
	value = strings.ReplaceAll(value, "&lt;", "<")
	value = strings.ReplaceAll(value, "&gt;", ">")
	return value
}
