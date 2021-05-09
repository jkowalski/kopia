// Package testenv contains Environment for use in testing.
package testenv

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/testutil"
)

const (
	// TestRepoPassword is a password for repositories created in tests.
	TestRepoPassword = "qWQPJ2hiiLgWRRCr"

	maxOutputLinesToLog = 4000
)

// CLIRunner encapsulates running kopia subcommands for testing purposes.
// It supports implementations that use subprocesses or in-process invocations.
type CLIRunner interface {
	Start(t *testing.T, args []string) (stdout, stderr io.Reader, wait func() error, kill func())
}

// CLITest encapsulates state for a CLI-based test.
type CLITest struct {
	startTime time.Time

	RepoDir   string
	ConfigDir string

	Runner CLIRunner

	fixedArgs []string

	DefaultRepositoryCreateFlags []string
}

// NewCLITest creates a new instance of *CLITest.
func NewCLITest(t *testing.T, runner CLIRunner) *CLITest {
	t.Helper()
	configDir := testutil.TempDirectory(t)

	fixedArgs := []string{
		// use per-test config file, to avoid clobbering current user's setup.
		"--config-file", filepath.Join(configDir, ".kopia.config"),
	}

	// disable the use of keyring
	switch runtime.GOOS {
	case "darwin":
		fixedArgs = append(fixedArgs, "--no-use-keychain")
	case "windows":
		fixedArgs = append(fixedArgs, "--no-use-credential-manager")
	case "linux":
		fixedArgs = append(fixedArgs, "--no-use-keyring")
	}

	var formatFlags []string

	if testutil.ShouldReduceTestComplexity() {
		formatFlags = []string{
			"--encryption", "CHACHA20-POLY1305-HMAC-SHA256",
			"--block-hash", "BLAKE2S-256",
		}
	}

	return &CLITest{
		startTime:                    clock.Now(),
		RepoDir:                      testutil.TempDirectory(t),
		ConfigDir:                    configDir,
		fixedArgs:                    fixedArgs,
		DefaultRepositoryCreateFlags: formatFlags,
		Runner:                       runner,
	}
}

func dumpLogs(t *testing.T, dirname string) {
	t.Helper()

	entries, err := ioutil.ReadDir(dirname)
	if err != nil {
		t.Errorf("unable to read %v: %v", dirname, err)

		return
	}

	for _, e := range entries {
		if e.IsDir() {
			dumpLogs(t, filepath.Join(dirname, e.Name()))
			continue
		}

		dumpLogFile(t, filepath.Join(dirname, e.Name()))
	}
}

func dumpLogFile(t *testing.T, fname string) {
	t.Helper()

	data, err := ioutil.ReadFile(fname)
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("LOG FILE: %v %v", fname, trimOutput(string(data)))
}

// RunAndExpectSuccess runs the given command, expects it to succeed and returns its output lines.
func (e *CLITest) RunAndExpectSuccess(t *testing.T, args ...string) []string {
	t.Helper()

	stdout, _, err := e.Run(t, false, args...)
	if err != nil {
		t.Fatalf("'kopia %v' failed with %v", strings.Join(args, " "), err)
	}

	return stdout
}

// RunAndProcessStderr runs the given command, and streams its output line-by-line to a given function until it returns false.
func (e *CLITest) RunAndProcessStderr(t *testing.T, callback func(line string) bool, args ...string) (kill func()) {
	t.Helper()

	stdout, stderr, _, kill := e.Runner.Start(t, e.cmdArgs(args))
	go io.Copy(io.Discard, stdout)

	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		if !callback(scanner.Text()) {
			break
		}
	}

	// complete the scan in background without processing lines.
	go func() {
		for scanner.Scan() {
			t.Logf("[stderr] %v", scanner.Text())
		}
	}()

	return kill
}

// RunAndExpectSuccessWithErrOut runs the given command, expects it to succeed and returns its stdout and stderr lines.
func (e *CLITest) RunAndExpectSuccessWithErrOut(t *testing.T, args ...string) (stdout, stderr []string) {
	t.Helper()

	stdout, stderr, err := e.Run(t, false, args...)
	if err != nil {
		t.Fatalf("'kopia %v' failed with %v", strings.Join(args, " "), err)
	}

	return stdout, stderr
}

// RunAndExpectFailure runs the given command, expects it to fail and returns its output lines.
func (e *CLITest) RunAndExpectFailure(t *testing.T, args ...string) []string {
	t.Helper()

	stdout, _, err := e.Run(t, true, args...)
	if err == nil {
		t.Fatalf("'kopia %v' succeeded, but expected failure", strings.Join(args, " "))
	}

	return stdout
}

// RunAndVerifyOutputLineCount runs the given command and asserts it returns the given number of output lines, then returns them.
func (e *CLITest) RunAndVerifyOutputLineCount(t *testing.T, wantLines int, args ...string) []string {
	t.Helper()

	lines := e.RunAndExpectSuccess(t, args...)
	if len(lines) != wantLines {
		t.Errorf("unexpected list of results of 'kopia %v': %v (%v lines), wanted %v", strings.Join(args, " "), lines, len(lines), wantLines)
	}

	return lines
}

func (e *CLITest) cmdArgs(args []string) []string {
	var suffix []string

	// detect repository creation and override DefaultRepositoryCreateFlags for best
	// performance on the current platform.
	if len(args) >= 2 && (args[0] == "repo" && args[1] == "create") {
		suffix = e.DefaultRepositoryCreateFlags
	}

	return append(append(append([]string(nil), e.fixedArgs...), args...), suffix...)
}

// Run executes kopia with given arguments and returns the output lines.
func (e *CLITest) Run(t *testing.T, expectedError bool, args ...string) (stdout, stderr []string, err error) {
	t.Helper()

	t.Logf("running 'kopia %v'", strings.Join(args, " "))
	stdoutReader, stderrReader, wait, _ := e.Runner.Start(t, e.cmdArgs(args))

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		scanner := bufio.NewScanner(stdoutReader)
		for scanner.Scan() {
			stdout = append(stdout, scanner.Text())
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()

		scanner := bufio.NewScanner(stderrReader)
		for scanner.Scan() {
			stderr = append(stderr, scanner.Text())
		}
	}()

	wg.Wait()

	gotErr := wait()

	if expectedError {
		require.Error(t, gotErr, "unexpected success when running 'kopia %v' (stdout:\n%v\nstderr:\n%v", strings.Join(args, " "), strings.Join(stdout, "\n"), strings.Join(stderr, "\n"))
	} else {
		require.NoError(t, gotErr, "unexpected error when running 'kopia %v' (stdout:\n%v\nstderr:\n%v", strings.Join(args, " "), strings.Join(stdout, "\n"), strings.Join(stderr, "\n"))
	}

	return stdout, stderr, gotErr
}

func trimOutput(s string) string {
	lines := splitLines(s)
	if len(lines) <= maxOutputLinesToLog {
		return s
	}

	lines2 := append([]string(nil), lines[0:(maxOutputLinesToLog/2)]...)
	lines2 = append(lines2, fmt.Sprintf("/* %v lines removed */", len(lines)-maxOutputLinesToLog))
	lines2 = append(lines2, lines[len(lines)-(maxOutputLinesToLog/2):]...)

	return strings.Join(lines2, "\n")
}

func splitLines(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}

	var result []string
	for _, l := range strings.Split(s, "\n") {
		result = append(result, strings.TrimRight(l, "\r"))
	}

	return result
}
