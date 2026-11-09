//go:build windows

package main

import "os"

// notifyReload is a no-op on Windows, where hot reloads are driven by the file watcher.
func notifyReload(_ chan<- os.Signal) {}
