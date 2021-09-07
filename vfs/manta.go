//go:build !nos3
// +build !nos3

package vfs

import (
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/eikenb/pipeat"
	"github.com/pkg/sftp"

	"github.com/drakkan/sftpgo/v2/version"
	triton "github.com/joyent/triton-go/v2"
	"github.com/joyent/triton-go/v2/authentication"
	"github.com/joyent/triton-go/v2/storage"
)

// MantaFs is a Fs implementation for Joyent Manta compatible object storages
type MantaFs struct {
	connectionID string
	localTempDir string
	// if not empty this fs is mouted as virtual folder in the specified path
	mountPath      string
	config         *MantaFsConfig
	svc            *storage.StorageClient
	ctxTimeout     time.Duration
	ctxLongTimeout time.Duration
}

func init() {
	version.AddFeature("+s3")
}

// NewS3Fs returns an S3Fs object that allows to interact with an s3 compatible
// object storage
func NewMantaFs(connectionID, localTempDir, mountPath string, config MantaFsConfig) (Fs, error) {
	if localTempDir == "" {
		if tempPath != "" {
			localTempDir = tempPath
		} else {
			localTempDir = filepath.Clean(os.TempDir())
		}
	}
	fs := &MantaFs{
		connectionID:   connectionID,
		localTempDir:   localTempDir,
		mountPath:      mountPath,
		config:         &config,
		ctxTimeout:     30 * time.Second,
		ctxLongTimeout: 300 * time.Second,
	}
	if err := fs.config.Validate(); err != nil {
		return fs, err
	}

	fs.svc = NewMantaClient(fs.config)
	return fs, nil
}

// Name returns the name for the Fs implementation
func (fs *MantaFs) Name() string {
	return fmt.Sprintf("MantaFs path %#v", fs.config.Path)
}

// ConnectionID returns the connection ID associated to this Fs implementation
func (fs *MantaFs) ConnectionID() string {
	return fs.connectionID
}

// Stat returns a FileInfo describing the named file
func (fs *MantaFs) Stat(name string) (os.FileInfo, error) {
	return nil, nil
}

// Lstat returns a FileInfo describing the named file
func (fs *MantaFs) Lstat(name string) (os.FileInfo, error) {
	return fs.Stat(name)
}

// Open opens the named file for reading
func (fs *MantaFs) Open(name string, offset int64) (File, *pipeat.PipeReaderAt, func(), error) {
	return nil, nil, nil, nil
}

// Create creates or opens the named file for writing
func (fs *MantaFs) Create(name string, flag int) (File, *PipeWriter, func(), error) {
	return nil, nil, nil, nil
}

func (fs *MantaFs) Rename(source, target string) error {
	return nil
}

// Remove removes the named file or (empty) directory.
func (fs *MantaFs) Remove(name string, isDir bool) error {
	return nil
}

// Mkdir creates a new directory with the specified name and default permissions
func (fs *MantaFs) Mkdir(name string) error {
	return nil
}

// MkdirAll does nothing, we don't have folder
func (*MantaFs) MkdirAll(name string, uid int, gid int) error {
	return nil
}

// Symlink creates source as a symbolic link to target.
func (*MantaFs) Symlink(source, target string) error {
	return ErrVfsUnsupported
}

// Readlink returns the destination of the named symbolic link
func (*MantaFs) Readlink(name string) (string, error) {
	return "", ErrVfsUnsupported
}

// Chown changes the numeric uid and gid of the named file.
func (*MantaFs) Chown(name string, uid int, gid int) error {
	return ErrVfsUnsupported
}

// Chmod changes the mode of the named file to mode.
func (*MantaFs) Chmod(name string, mode os.FileMode) error {
	return ErrVfsUnsupported
}

// Chtimes changes the access and modification times of the named file.
func (*MantaFs) Chtimes(name string, atime, mtime time.Time) error {
	return ErrVfsUnsupported
}

// Truncate changes the size of the named file.
// Truncate by path is not supported, while truncating an opened
// file is handled inside base transfer
func (*MantaFs) Truncate(name string, size int64) error {
	return ErrVfsUnsupported
}

// ReadDir reads the directory named by dirname and returns
// a list of directory entries.
func (fs *MantaFs) ReadDir(dirname string) ([]os.FileInfo, error) {
	return nil, nil
}

// IsUploadResumeSupported returns true if resuming uploads is supported.
// Resuming uploads is not supported on S3
func (*MantaFs) IsUploadResumeSupported() bool {
	return false
}

// IsAtomicUploadSupported returns true if atomic upload is supported.
// S3 uploads are already atomic, we don't need to upload to a temporary
// file
func (*MantaFs) IsAtomicUploadSupported() bool {
	return false
}

// IsNotExist returns a boolean indicating whether the error is known to
// report that a file or directory does not exist
func (*MantaFs) IsNotExist(err error) bool {
	return false
}

// IsPermission returns a boolean indicating whether the error is known to
// report that permission is denied.
func (*MantaFs) IsPermission(err error) bool {
	return false
}

// IsNotSupported returns true if the error indicate an unsupported operation
func (*MantaFs) IsNotSupported(err error) bool {
	if err == nil {
		return false
	}
	return err == ErrVfsUnsupported
}

// CheckRootPath creates the specified local root directory if it does not exists
func (fs *MantaFs) CheckRootPath(username string, uid int, gid int) bool {
	// we need a local directory for temporary files
	osFs := NewOsFs(fs.ConnectionID(), fs.localTempDir, "")
	return osFs.CheckRootPath(username, uid, gid)
}

// ScanRootDirContents returns the number of files contained in the bucket,
// and their size
func (fs *MantaFs) ScanRootDirContents() (int, int64, error) {
	return 2, 2, nil
}

// GetDirSize returns the number of files and the size for a folder
// including any subfolders
func (*MantaFs) GetDirSize(dirname string) (int, int64, error) {
	return 0, 0, nil
}

// GetAtomicUploadPath returns the path to use for an atomic upload.
// S3 uploads are already atomic, we never call this method for Manta
func (*MantaFs) GetAtomicUploadPath(name string) string {
	return ""
}

// GetRelativePath returns the path for a file relative to the user's home dir.
// This is the path as seen by SFTPGo users
func (fs *MantaFs) GetRelativePath(name string) string {
	return "/rawr"
}

// Walk walks the file tree rooted at root, calling walkFn for each file or
// directory in the tree, including root. The result are unordered
func (fs *MantaFs) Walk(root string, walkFn filepath.WalkFunc) error {
	return nil
}

// Join joins any number of path elements into a single path
func (*MantaFs) Join(elem ...string) string {
	return path.Join(elem...)
}

// HasVirtualFolders returns true if folders are emulated
func (*MantaFs) HasVirtualFolders() bool {
	return true
}

// ResolvePath returns the matching filesystem path for the specified virtual path
func (fs *MantaFs) ResolvePath(virtualPath string) (string, error) {
	return "", nil
}

// GetMimeType returns the content type
func (fs *MantaFs) GetMimeType(name string) (string, error) {
	return "rawr", nil
}

// Close closes the fs
func (*MantaFs) Close() error {
	return nil
}

// GetAvailableDiskSize return the available size for the specified path
func (*MantaFs) GetAvailableDiskSize(dirName string) (*sftp.StatVFS, error) {
	return nil, ErrStorageSizeUnavailable
}

func NewMantaClient(config *MantaFsConfig) *storage.StorageClient {
	accountName := config.Account
	keyMaterial := config.KeyMaterial
	keyID := config.KeyId
	userName := config.User

	var signer authentication.Signer
	var err error

	if keyMaterial == "" {
		input := authentication.SSHAgentSignerInput{
			KeyID:       keyID,
			AccountName: accountName,
			Username:    userName,
		}
		signer, err = authentication.NewSSHAgentSigner(input)
		if err != nil {
			log.Fatalf("Error Creating SSH Agent Signer: {{err}}", err)
		}
	} else {
		var keyBytes []byte
		if _, err = os.Stat(keyMaterial); err == nil {
			keyBytes, err = ioutil.ReadFile(keyMaterial)
			if err != nil {
				log.Fatalf("Error reading key material from %s: %s",
					keyMaterial, err)
			}
			block, _ := pem.Decode(keyBytes)
			if block == nil {
				log.Fatalf(
					"Failed to read key material '%s': no key found", keyMaterial)
			}

			if block.Headers["Proc-Type"] == "4,ENCRYPTED" {
				log.Fatalf(
					"Failed to read key '%s': password protected keys are\n"+
						"not currently supported. Please decrypt the key prior to use.", keyMaterial)
			}

		} else {
			keyBytes = []byte(keyMaterial)
		}

		input := authentication.PrivateKeySignerInput{
			KeyID:              keyID,
			PrivateKeyMaterial: keyBytes,
			AccountName:        accountName,
			Username:           userName,
		}
		signer, err = authentication.NewPrivateKeySigner(input)
		if err != nil {
			log.Fatalf("Error Creating SSH Private Key Signer: {{err}}", err)
		}
	}

	cfg := &triton.ClientConfig{
		MantaURL:    config.URL,
		AccountName: config.Account,
		Username:    "",
		Signers:     []authentication.Signer{signer},
	}

	c, err := storage.NewClient(cfg)
	if err != nil {
		log.Fatalf("compute.NewClient: %s", err)
	}

	return c
}
