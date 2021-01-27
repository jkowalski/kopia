package sftp_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sftp"
)

const (
	dockerImage  = "atmoz/sftp"
	dialTimeout  = 10 * time.Second
	sftpUsername = "foo"
)

func mustGetLocalTmpDir(t *testing.T) string {
	t.Helper()

	tmpDir, err := ioutil.TempDir(".", ".creds")
	if err != nil {
		t.Fatal(err)
	}

	tmpDir, err = filepath.Abs(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	return tmpDir
}

func mustRunOrSkip(t *testing.T, cmd string, args ...string) []byte {
	t.Helper()

	o, err := exec.Command(cmd, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v", o, err)
	}

	t.Logf("output: %s", o)

	return o
}

func startDockerSFTPServerOrSkip(t *testing.T, idRSA string) (host string, port int, knownHostsFile string) {
	t.Helper()

	tmpDir := mustGetLocalTmpDir(t)
	sshHostED25519Key := filepath.Join(tmpDir, "ssh_host_ed25519_key")
	sshHostRSAKey := filepath.Join(tmpDir, "ssh_host_rsa_key")

	mustRunOrSkip(t, "ssh-keygen", "-t", "ed25519", "-P", "", "-f", sshHostED25519Key)
	mustRunOrSkip(t, "ssh-keygen", "-t", "rsa", "-P", "", "-f", sshHostRSAKey)

	// see https://github.com/atmoz/sftp for instructions
	shortContainerID := testutil.RunContainerAndKillOnCloseOrSkip(t,
		"run", "--rm", "-p", "0:22",
		"-v", idRSA+".pub:/home/"+sftpUsername+"/.ssh/keys/id_rsa.pub:ro",
		"-v", sshHostED25519Key+":/etc/ssh/ssh_host_ed25519_key:ro",
		"-v", sshHostRSAKey+":/etc/ssh/ssh_host_rsa_key:ro",
		"-d", dockerImage,
		sftpUsername+"::::upload")
	sftpEndpoint := testutil.GetContainerMappedPortAddress(t, shortContainerID, "22")

	// wait for SFTP server to come up.
	deadline := time.Now().Add(dialTimeout)
	for time.Now().Before(deadline) {
		t.Logf("waiting for SFTP server to come up on '%v'...", sftpEndpoint)

		conn, err := net.DialTimeout("tcp", sftpEndpoint, time.Second)
		if err != nil {
			t.Logf("err: %v", err)
			time.Sleep(time.Second)

			continue
		}

		defer conn.Close()

		parts := strings.Split(sftpEndpoint, ":")
		host = parts[0]
		port, _ = strconv.Atoi(parts[1])
		knownHostsFile = filepath.Join(t.TempDir(), "known_hosts")
		knownHostsData := mustRunOrSkip(t, "ssh-keyscan", "-t", "rsa", "-p", strconv.Itoa(port), host)

		ioutil.WriteFile(knownHostsFile, knownHostsData, 0600)

		t.Logf("SFTP server OK on host:%q port:%v. Known hosts file: %v", host, port, knownHostsFile)

		return
	}

	t.Skipf("SFTP server did not start!")

	return //nolint:nakedret
}

func TestSFTPStorageValid(t *testing.T) {
	testutil.TestSkipOnCIUnlessLinuxAMD64(t)

	tmpDir := mustGetLocalTmpDir(t)
	idRSA := filepath.Join(tmpDir, "id_rsa")

	mustRunOrSkip(t, "ssh-keygen", "-t", "rsa", "-P", "", "-f", idRSA)

	host, port, knownHostsFile := startDockerSFTPServerOrSkip(t, idRSA)

	for _, embedCreds := range []bool{false, true} {
		embedCreds := embedCreds
		t.Run(fmt.Sprintf("Embed=%v", embedCreds), func(t *testing.T) {
			ctx := testlogging.Context(t)

			st, err := createSFTPStorage(ctx, t, host, port, idRSA, knownHostsFile, embedCreds)
			if err != nil {
				t.Fatalf("unable to connect to SSH: %v", err)
			}

			deleteBlobs(ctx, t, st)

			blobtesting.VerifyStorage(ctx, t, st)
			blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)

			// delete everything again
			deleteBlobs(ctx, t, st)

			if err := st.Close(ctx); err != nil {
				t.Fatalf("err: %v", err)
			}
		})
	}
}

func TestInvalidServerFailsFast(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	tmpDir := mustGetLocalTmpDir(t)
	idRSA := filepath.Join(tmpDir, "id_rsa")
	knownHostsFile := filepath.Join(tmpDir, "known_hosts")

	mustRunOrSkip(t, "ssh-keygen", "-t", "rsa", "-P", "", "-f", idRSA)
	ioutil.WriteFile(knownHostsFile, nil, 0600)

	t0 := time.Now()

	if _, err := createSFTPStorage(ctx, t, "no-such-host", 22, idRSA, knownHostsFile, false); err == nil {
		t.Fatalf("unexpected success with bad credentials")
	}

	if dt := time.Since(t0); dt > 10*time.Second {
		t.Fatalf("opening storage took too long, probably due to retries")
	}
}

func deleteBlobs(ctx context.Context, t *testing.T, st blob.Storage) {
	t.Helper()

	if err := st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		return st.DeleteBlob(ctx, bm.BlobID)
	}); err != nil {
		t.Fatalf("unable to clear sftp storage: %v", err)
	}
}

func createSFTPStorage(ctx context.Context, t *testing.T, host string, port int, idRSA, knownHostsFile string, embed bool) (blob.Storage, error) {
	t.Helper()

	if _, err := os.Stat(knownHostsFile); err != nil {
		t.Fatalf("skipping test because SFTP known hosts file can't be opened: %v", knownHostsFile)
	}

	opt := &sftp.Options{
		Path:           "/upload",
		Host:           host,
		Username:       sftpUsername,
		Port:           port,
		Keyfile:        idRSA,
		KnownHostsFile: knownHostsFile,
	}

	if embed {
		opt.KeyData = mustReadFileToString(t, opt.Keyfile)
		opt.Keyfile = ""

		opt.KnownHostsData = mustReadFileToString(t, opt.KnownHostsFile)
		opt.KnownHostsFile = ""
	}

	return sftp.New(ctx, opt)
}

func mustReadFileToString(t *testing.T, fname string) string {
	t.Helper()

	data, err := ioutil.ReadFile(fname)
	if err != nil {
		t.Fatal(err)
	}

	return string(data)
}
