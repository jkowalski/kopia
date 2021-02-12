// Package atomicfile provides wrappers for atomically writing files in a manner compatible with long filenames.
package atomicfile

import (
	"io"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/natefinch/atomic"
)

const maxPathLength = 260

// MaybePrefixLongFilenameOnWindows prefixes the given filename with \\?\ on Windows
// if the filename is longer than 260 characters, which is required to be able to
// use some low-level Windows APIs.
// Because long file names have certain limitations:
// - we must replace forward slashes with backslashes.
// - dummy path element (\.\) must be removed.
func MaybePrefixLongFilenameOnWindows(fname string) string {
	if runtime.GOOS != "windows" {
		return fname
	}

	if len(fname) < maxPathLength {
		return fname
	}

	if !filepath.IsAbs(fname) {
		// only convert relative paths
		return fname
	}

	fixed := strings.ReplaceAll(fname, "/", "\\")

	for {
		fixed2 := strings.ReplaceAll(fixed, "\\.\\", "\\")
		if fixed2 == fixed {
			break
		}

		fixed = fixed2
	}

	return "\\\\?\\" + fixed
}

// Write is a wrapper around atomic.WriteFile that handles long file names on Windows.
func Write(filename string, r io.Reader) error {
	return atomic.WriteFile(MaybePrefixLongFilenameOnWindows(filename), r)
}
