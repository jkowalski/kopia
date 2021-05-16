package endtoend_test

import (
	"io/ioutil"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestCompression(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	// set global policy
	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--compression", "pgzip")

	dataDir := testutil.TempDirectory(t)

	dataLines := []string{
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
	}

	// add a file that compresses well
	require.NoError(t, ioutil.WriteFile(filepath.Join(dataDir, "some-file1"), []byte(strings.Join(dataLines, "\n")), 0o600))

	e.RunAndExpectSuccess(t, "snapshot", "create", dataDir)
	sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)
	oid := sources[0].Snapshots[0].ObjectID
	entries := clitestutil.ListDirectory(t, e, oid)

	supportsContentLevelCompression := containsLine(
		e.RunAndExpectSuccess(t, "repo", "status"),
		"Content compression: true",
	)

	// without content-level compression, we'll do it at object level and object ID will be prefixed with 'Z'
	if !supportsContentLevelCompression {
		if !strings.HasPrefix(entries[0].ObjectID, "Z") {
			t.Errorf("expected compressed object, got %v", entries[0].ObjectID)
		}
	} else {
		// with content-level compression we're looking for a content with compression.
		lines := e.RunAndExpectSuccess(t, "content", "ls", "-c")
		found := false

		for _, l := range lines {
			if strings.HasPrefix(l, entries[0].ObjectID) {
				require.Contains(t, l, "pgzip")
				found = true
			}
		}

		require.True(t, found)
	}

	if lines := e.RunAndExpectSuccess(t, "show", entries[0].ObjectID); !reflect.DeepEqual(dataLines, lines) {
		t.Errorf("invalid object contents")
	}
}

func containsLine(lines []string, line string) bool {
	for _, l := range lines {
		if line == l {
			return true
		}
	}

	return false
}
