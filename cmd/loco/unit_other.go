//go:build !darwin && !linux

package main

// serviceUnitExecutable is unavailable on platforms without a service manager.
func serviceUnitExecutable(string) string { return "" }
