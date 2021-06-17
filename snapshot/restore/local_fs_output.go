// Package restore manages restoring filesystem snapshots.
package restore

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/atomicfile"
	"github.com/kopia/kopia/snapshot"
)

const (
	modBits                           = os.ModePerm | os.ModeSetgid | os.ModeSetuid | os.ModeSticky
	outputDirMode                     = 0o700 // default mode to create directories in before setting their ACLs
	maxTimeDeltaToConsiderFileTheSame = 2 * time.Second
)

// FilesystemOutput contains the options for outputting a file system tree.
type FilesystemOutput struct {
	// TargetPath for restore.
	TargetPath string `json:"targetPath"`

	// If a directory already exists, overwrite the directory.
	OverwriteDirectories bool `json:"overwriteDirectories"`

	// Indicate whether or not to overwrite existing files. When set to false,
	// the copier does not modify already existing files and returns an error
	// instead.
	OverwriteFiles bool `json:"overwriteFiles"`

	// If a symlink already exists, remove it and create a new one. When set to
	// false, the copier does not modify existing symlinks and will return an
	// error instead.
	OverwriteSymlinks bool `json:"overwriteSymlinks"`

	// IgnorePermissionErrors causes restore to ignore errors due to invalid permissions.
	IgnorePermissionErrors bool `json:"ignorePermissionErrors"`

	// SkipOwners when set to true causes restore to skip restoring owner information.
	SkipOwners bool `json:"skipOwners"`

	// SkipPermissions when set to true causes restore to skip restoring permission information.
	SkipPermissions bool `json:"skipPermissions"`

	// SkipTimes when set to true causes restore to skip restoring modification times.
	SkipTimes bool `json:"skipTimes"`
}

// Parallelizable implements restore.Output interface.
func (o *FilesystemOutput) Parallelizable() bool {
	return true
}

// BeginDirectory implements restore.Output interface.
func (o *FilesystemOutput) BeginDirectory(ctx context.Context, relativePath string, e fs.Directory) error {
	path := filepath.Join(o.TargetPath, filepath.FromSlash(relativePath))

	if err := o.createDirectory(ctx, path); err != nil {
		return errors.Wrap(err, "error creating directory")
	}

	return nil
}

// FinishDirectory implements restore.Output interface.
func (o *FilesystemOutput) FinishDirectory(ctx context.Context, relativePath string, e fs.Directory) error {
	path := filepath.Join(o.TargetPath, filepath.FromSlash(relativePath))
	if err := o.setAttributes(path, e, os.FileMode(0)); err != nil {
		return errors.Wrap(err, "error setting attributes")
	}

	return SafeRemoveAll(path)
}

// WriteDirEntry implements restore.Output interface.
func (o *FilesystemOutput) WriteDirEntry(ctx context.Context, relativePath string, de *snapshot.DirEntry, e fs.Directory) error {
	return nil
}

// Close implements restore.Output interface.
func (o *FilesystemOutput) Close(ctx context.Context) error {
	return nil
}

// WriteFile implements restore.Output interface.
func (o *FilesystemOutput) WriteFile(ctx context.Context, relativePath string, f fs.File) error {
	log(ctx).Debugf("WriteFile %v (%v bytes) %v, %v", filepath.Join(o.TargetPath, relativePath), f.Size(), f.Mode(), f.ModTime())
	path := filepath.Join(o.TargetPath, filepath.FromSlash(relativePath))

	if err := o.copyFileContent(ctx, path, f); err != nil {
		return errors.Wrap(err, "error creating file")
	}

	if err := o.setAttributes(path, f, os.FileMode(0)); err != nil {
		return errors.Wrap(err, "error setting attributes")
	}

	return SafeRemoveAll(path)
}

// FileExists implements restore.Output interface.
func (o *FilesystemOutput) FileExists(ctx context.Context, relativePath string, e fs.File) bool {
	st, err := os.Lstat(filepath.Join(o.TargetPath, relativePath))
	if err != nil {
		return false
	}

	if (st.Mode() & os.ModeType) != 0 {
		// not a file
		return false
	}

	if st.Size() != e.Size() {
		// wrong size
		return false
	}

	timeDelta := st.ModTime().Sub(e.ModTime())
	if timeDelta < 0 {
		timeDelta = -timeDelta
	}

	return timeDelta < maxTimeDeltaToConsiderFileTheSame
}

// CreateSymlink implements restore.Output interface.
func (o *FilesystemOutput) CreateSymlink(ctx context.Context, relativePath string, e fs.Symlink) error {
	targetPath, err := e.Readlink(ctx)
	if err != nil {
		return errors.Wrap(err, "error reading link target")
	}

	log(ctx).Debugf("CreateSymlink %v => %v, time %v", filepath.Join(o.TargetPath, relativePath), targetPath, e.ModTime())

	path := filepath.Join(o.TargetPath, filepath.FromSlash(relativePath))

	switch stat, err := os.Lstat(path); {
	case os.IsNotExist(err): // Proceed to symlink creation
	case err != nil:
		return errors.Wrap(err, "lstat error at symlink path")
	case fileIsSymlink(stat):
		// Throw error if we are not overwriting symlinks
		if !o.OverwriteSymlinks {
			return errors.Errorf("will not overwrite existing symlink")
		}

		// Remove the existing symlink before symlink creation
		if err := os.Remove(path); err != nil {
			return errors.Wrap(err, "removing existing symlink")
		}
	default:
		return errors.Errorf("unable to create symlink, %q already exists and is not a symlink", path)
	}

	if err := os.Symlink(targetPath, path); err != nil {
		return errors.Wrap(err, "error creating symlink")
	}

	if err := o.setAttributes(path, e, os.FileMode(0)); err != nil {
		return errors.Wrap(err, "error setting attributes")
	}

	return nil
}

func fileIsSymlink(stat os.FileInfo) bool {
	return stat.Mode()&os.ModeSymlink != 0
}

// SymlinkExists implements restore.Output interface.
func (o *FilesystemOutput) SymlinkExists(ctx context.Context, relativePath string, e fs.Symlink) bool {
	st, err := os.Lstat(filepath.Join(o.TargetPath, relativePath))
	if err != nil {
		return false
	}

	return (st.Mode() & os.ModeType) == os.ModeSymlink
}

// setAttributes sets permission, modification time and user/group ids
// on targetPath. modclear will clear the specified FileMod bits. Pass 0
// to not clear any.
func (o *FilesystemOutput) setAttributes(targetPath string, e fs.Entry, modclear os.FileMode) error {
	le, err := localfs.NewEntry(targetPath)
	if err != nil {
		return errors.Wrap(err, "could not create local FS entry for "+targetPath)
	}

	var (
		osChmod   = os.Chmod
		osChown   = os.Chown
		osChtimes = os.Chtimes
	)

	// symbolic links require special handling that is OS-specific and sometimes unsupported
	// os.* functions change the target of the symlink and not the symlink itself.
	if isSymlink(e) {
		osChmod, osChown, osChtimes = symlinkChmod, symlinkChown, symlinkChtimes
		modclear = os.FileMode(0)
	}

	// Set owner user and group from e
	// On Windows Chown is not supported. fs.OwnerInfo collected on Windows will always
	// be zero-value for UID and GID, so the Chown operation is not performed.
	if o.shouldUpdateOwner(le, e) {
		if err = o.maybeIgnorePermissionError(osChown(targetPath, int(e.Owner().UserID), int(e.Owner().GroupID))); err != nil {
			return errors.Wrap(err, "could not change owner/group for "+targetPath)
		}
	}

	// Set file permissions from e
	if o.shouldUpdatePermissions(le, e, modclear) {
		if err = o.maybeIgnorePermissionError(osChmod(targetPath, (e.Mode()&modBits)&^modclear)); err != nil {
			return errors.Wrap(err, "could not change permissions on "+targetPath)
		}
	}

	if o.shouldUpdateTimes(le, e) {
		if err = o.maybeIgnorePermissionError(osChtimes(targetPath, e.ModTime(), e.ModTime())); err != nil {
			return errors.Wrap(err, "could not change mod time on "+targetPath)
		}
	}

	return nil
}

func isSymlink(e fs.Entry) bool {
	_, ok := e.(fs.Symlink)
	return ok
}

func (o *FilesystemOutput) maybeIgnorePermissionError(err error) error {
	if o.IgnorePermissionErrors && os.IsPermission(err) {
		return nil
	}

	return err
}

func (o *FilesystemOutput) shouldUpdateOwner(local, remote fs.Entry) bool {
	if o.SkipOwners {
		return false
	}

	if isWindows() {
		return false
	}

	return local.Owner() != remote.Owner()
}

func (o *FilesystemOutput) shouldUpdatePermissions(local, remote fs.Entry, modclear os.FileMode) bool {
	if o.SkipPermissions {
		return false
	}

	return ((local.Mode() & modBits) &^ modclear) != (remote.Mode() & modBits)
}

func (o *FilesystemOutput) shouldUpdateTimes(local, remote fs.Entry) bool {
	if o.SkipTimes {
		return false
	}

	return !local.ModTime().Equal(remote.ModTime())
}

func isWindows() bool {
	return runtime.GOOS == "windows"
}

func (o *FilesystemOutput) createDirectory(ctx context.Context, path string) error {
	switch stat, err := os.Stat(path); {
	case os.IsNotExist(err):
		// nolint:wrapcheck
		return os.MkdirAll(path, outputDirMode)
	case err != nil:
		return errors.Wrap(err, "failed to stat path "+path)
	case stat.Mode().IsDir():
		if !o.OverwriteDirectories {
			if empty, _ := isEmptyDirectory(path); !empty {
				return errors.Errorf("non-empty directory already exists, not overwriting it: %q", path)
			}
		}

		log(ctx).Debugf("Not creating already existing directory: %v", path)

		return nil
	default:
		return errors.Errorf("unable to create directory, %q already exists and it is not a directory", path)
	}
}

func (o *FilesystemOutput) copyFileContent(ctx context.Context, targetPath string, f fs.File) error {
	switch _, err := os.Stat(targetPath); {
	case os.IsNotExist(err): // copy file below
	case err == nil:
		if !o.OverwriteFiles {
			return errors.Errorf("unable to create %q, it already exists", targetPath)
		}

		log(ctx).Debugf("Overwriting existing file: %v", targetPath)
	default:
		return errors.Wrap(err, "failed to stat "+targetPath)
	}

	r, err := f.Open(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to open snapshot file for "+targetPath)
	}
	defer r.Close() //nolint:errcheck

	log(ctx).Debugf("copying file contents to: %v", targetPath)

	// nolint:wrapcheck
	return atomicfile.Write(targetPath, r)
}

func isEmptyDirectory(name string) (bool, error) {
	f, err := os.Open(name) //nolint:gosec
	if err != nil {
		return false, errors.Wrap(err, "error opening directory")
	}

	defer f.Close() //nolint:errcheck,gosec

	if _, err = f.Readdirnames(1); errors.Is(err, io.EOF) {
		return true, nil
	}

	return false, errors.Wrap(err, "error reading directory") // Either not empty or error
}

var _ Output = (*FilesystemOutput)(nil)
