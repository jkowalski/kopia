package repo

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/zalando/go-keyring"
)

// KeyRingEnabled enables password persistence uses OS-specific keyring.
var KeyRingEnabled = false

// GetPersistedPassword retrieves persisted password for a given repository config.
func GetPersistedPassword(configFile string) (string, bool) {
	if KeyRingEnabled {
		kr, err := keyring.Get(getKeyringItemID(configFile), keyringUsername())
		if err == nil {
			log.Debugf("password for %v retrieved from OS keyring", configFile)
			return kr, true
		}
	}

	b, err := ioutil.ReadFile(passwordFileName(configFile))
	if err == nil {
		s, err := base64.StdEncoding.DecodeString(string(b))
		if err == nil {
			log.Debugf("password for %v retrieved from password file", configFile)
			return string(s), true
		}
	}

	log.Debugf("could not find persisted password")

	return "", false
}

// persistPassword stores password for a given repository config.
func persistPassword(configFile, password string) error {
	if KeyRingEnabled {
		log.Debugf("saving password to OS keyring...")

		err := keyring.Set(getKeyringItemID(configFile), keyringUsername(), password)
		if err == nil {
			log.Infof("Saved password")
			return nil
		}

		return err
	}

	fn := passwordFileName(configFile)
	log.Infof("Saving password to file %v.", fn)

	return ioutil.WriteFile(fn, []byte(base64.StdEncoding.EncodeToString([]byte(password))), 0600)
}

// deletePassword removes stored repository password.
func deletePassword(configFile string) {
	// delete from both keyring and a file
	if KeyRingEnabled {
		err := keyring.Delete(getKeyringItemID(configFile), keyringUsername())
		if err == nil {
			log.Infof("deleted repository password for %v.", configFile)
		} else if err != keyring.ErrNotFound {
			log.Warningf("unable to delete keyring item %v: %v", getKeyringItemID(configFile), err)
		}
	}

	_ = os.Remove(passwordFileName(configFile))
}

func getKeyringItemID(configFile string) string {
	h := sha256.New()
	io.WriteString(h, configFile) //nolint:errcheck

	return fmt.Sprintf("%v-%x", filepath.Base(configFile), h.Sum(nil)[0:8])
}

func passwordFileName(configFile string) string {
	return configFile + ".kopia-password"
}

func keyringUsername() string {
	currentUser, err := user.Current()
	if err != nil {
		log.Warningf("Cannot determine keyring username: %s", err)
		return "nobody"
	}

	u := currentUser.Username

	if runtime.GOOS == "windows" {
		if p := strings.Index(u, "\\"); p >= 0 {
			// On Windows ignore domain name.
			u = u[p+1:]
		}
	}

	return u
}
