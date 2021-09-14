//go:build !nos3
// +build !nos3

package vfs

import (
	"context"
	"fmt"
	"io"
	"log"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/eikenb/pipeat"
	"github.com/pkg/sftp"

	"github.com/drakkan/sftpgo/v2/logger"
	"github.com/drakkan/sftpgo/v2/metric"
	"github.com/drakkan/sftpgo/v2/version"
	triton "github.com/joyent/triton-go/v2"
	"github.com/joyent/triton-go/v2/authentication"
	"github.com/joyent/triton-go/v2/storage"
)

const (
	rpath = "/stor"
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
	version.AddFeature("+manta")
}

// NewMantaFs returns an Manta client that allows to interact with an Manta
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

	var err error
	fs.svc, err = NewMantaClient(fs.config)
	if err != nil {
		return fs, err
	}

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
	if name == "" || name == "." {
		if fs.svc != nil {
			return nil, fmt.Errorf("Empty Name")
		}
		return NewFileInfo(name, true, 0, time.Now(), false), nil
	}

	info, err := fs.headObject(name)
	if err != nil {
		return nil, err
	}

	isDir := false
	size := info.ContentLength
	if strings.HasSuffix(info.ContentType, "type=directory") {
		isDir = true
		size = info.ResultSetSize
	}

	return NewFileInfo(name, isDir, int64(size), info.LastModified, false), nil
}

func (fs *MantaFs) headObject(name string) (*storage.GetInfoOutput, error) {
	ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(fs.ctxTimeout))
	defer cancelFn()
	info, err := fs.svc.Objects().GetInfo(ctx, &storage.GetInfoInput{
		ObjectPath: name,
	})
	if err != nil {
		return nil, err
	}

	metric.MantaHeadObjectCompleted(err)
	return info, nil
}

// Lstat returns a FileInfo describing the named file
func (fs *MantaFs) Lstat(name string) (os.FileInfo, error) {
	return fs.Stat(name)
}

// Open opens the named file for reading
func (fs *MantaFs) Open(name string, offset int64) (File, *pipeat.PipeReaderAt, func(), error) {
	r, w, err := pipeat.PipeInDir(fs.localTempDir)
	if err != nil {
		return nil, nil, nil, err
	}
	ctx, cancelFn := context.WithCancel(context.Background())

	obj, err := fs.svc.Objects().Get(ctx, &storage.GetObjectInput{
		ObjectPath: name,
	})

	if err != nil {
		r.Close()
		w.Close()
		cancelFn()
		return nil, nil, nil, err
	}

	go func() {
		defer cancelFn()
		defer obj.ObjectReader.Close()

		n, err := io.Copy(w, obj.ObjectReader)
		w.CloseWithError(err) //nolint:errcheck
		fsLog(fs, logger.LevelDebug, "download completed, path: %#v size: %v, err: %v", name, n, err)
		metric.MantaTransferCompleted(n, 1, err)
	}()

	return nil, r, cancelFn, nil
}

// Create creates or opens the named file for writing
func (fs *MantaFs) Create(name string, flag int) (File, *PipeWriter, func(), error) {
	r, w, err := pipeat.PipeInDir(fs.localTempDir)
	if err != nil {
		return nil, nil, nil, err
	}
	p := NewPipeWriter(w)

	ctx, cancelFn := context.WithCancel(context.Background())
	ct := mime.TypeByExtension(path.Ext(name))

	go func() {
		key := name
		defer cancelFn()
		var err error
		if flag == -1 {
			err = fs.svc.Dir().Put(ctx, &storage.PutDirectoryInput{
				DirectoryName: key,
			})
			r.CloseWithError(err) //nolint:errcheck
		} else {
			err = fs.svc.Objects().Put(ctx, &storage.PutObjectInput{
				ObjectPath:   key,
				ObjectReader: r,
				ForceInsert:  true,
				Headers: map[string]string{
					"Content-Type": ct,
				},
			})
		}
		p.Done(err)
		fsLog(fs, logger.LevelDebug, "upload completed, path: %#v, readed bytes: %v, err: %v", name, r.GetReadedBytes(), err)
		metric.MantaTransferCompleted(r.GetReadedBytes(), 0, err)
	}()

	return nil, p, cancelFn, nil
}

func (fs *MantaFs) Rename(source, target string) error {
	if fs.config.V2 {
		return ErrVfsUnsupported
	}
	ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(fs.ctxTimeout))
	defer cancelFn()
	err := fs.mkLink(ctx, source, target)
	if err != nil {
		return err
	}
	err = fs.Remove(source, false)
	return err
}

func (fs *MantaFs) mkLink(ctx context.Context, source, target string) error {
	err := fs.svc.SnapLinks().Put(ctx, &storage.PutSnapLinkInput{
		SourcePath: source,
		LinkPath:   target,
	})

	return err
}

// Remove removes the named file or (empty) directory.
func (fs *MantaFs) Remove(name string, isDir bool) error {
	ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(fs.ctxTimeout))
	defer cancelFn()
	err := fs.svc.Dir().Delete(ctx, &storage.DeleteDirectoryInput{
		DirectoryName: name,
	})
	metric.MantaDeleteObjectCompleted(err)
	return err
}

// Mkdir creates a new directory with the specified name and default permissions
func (fs *MantaFs) Mkdir(name string) error {
	_, err := fs.Stat(name)
	if !fs.IsNotExist(err) {
		return err
	}
	_, w, _, err := fs.Create(name, -1)
	if err != nil {
		return err
	}
	return w.Close()
}

// MkdirAll acts like mkdir -p
func (fs *MantaFs) MkdirAll(name string, uid int, gid int) error {
	// Fast path: does the directory already exist?
	dir, err := fs.Stat(name)
	if err == nil {
		if dir.IsDir() {
			return nil
		}
		return &os.PathError{Op: "mkdir", Path: name, Err: syscall.ENOTDIR}
	}
	// Slow path: make sure parent exists and then call Mkdir for path.
	i := len(name)
	for i > 0 && os.IsPathSeparator(name[i-1]) { // Skip trailing path separator.
		i--
	}

	j := i
	for j > 0 && !os.IsPathSeparator(name[j-1]) { // Scan backward over element.
		j--
	}

	if j > 1 {
		// Create parent.
		err = fs.MkdirAll(fixRootDirectory(name[:j-1]), uid, gid)
		if err != nil {
			return err
		}
	}

	// Parent now exists; invoke Mkdir and use its result.
	err = fs.Mkdir(name)
	if err != nil {
		// Handle arguments like "foo/." by
		// double-checking that directory doesn't exist.
		dir, err1 := fs.Lstat(name)
		if err1 == nil && dir.IsDir() {
			return nil
		}
		return err
	}

	return nil
}

func fixRootDirectory(p string) string {
	return p
}

// Symlink creates source as a symbolic link to target.
func (fs *MantaFs) Symlink(source, target string) error {
	if fs.config.V2 {
		return ErrVfsUnsupported
	}
	ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(fs.ctxTimeout))
	defer cancelFn()
	err := fs.mkLink(ctx, source, target)
	metric.MantaSymLinkObjectCompleted(err)
	return err
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
	ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(fs.ctxTimeout))
	defer cancelFn()
	dirList, err := fs.svc.Dir().List(ctx, &storage.ListDirectoryInput{
		DirectoryName: dirname,
	})
	if err != nil {
		return nil, err
	}

	var result []os.FileInfo
	for _, e := range dirList.Entries {
		dir := false
		if e.Type == "directory" {
			dir = true
		}
		result = append(result, NewFileInfo(e.Name, dir, int64(e.Size), e.ModifiedTime, false))
	}
	metric.MantaListObjectsCompleted(err)
	return result, err
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
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "404")

}

// IsPermission returns a boolean indicating whether the error is known to
// report that permission is denied.
func (*MantaFs) IsPermission(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "403")
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
	osFs.CheckRootPath(username, uid, gid)
	if fs.config.Path == "/" {
		return true
	}
	if err := fs.MkdirAll(rpath+fs.config.Path, uid, gid); err != nil {
		fsLog(fs, logger.LevelDebug, "error creating root directory %#v for user %#v: %v", fs.config.Path, username, err)
		return false
	}
	// we need a local directory for temporary files
	return true
}

// ScanRootDirContents returns the number of files contained in the bucket,
// and their size
func (fs *MantaFs) ScanRootDirContents() (int, int64, error) {
	return fs.getFileCounts()
}

func (fs *MantaFs) getFileCounts() (int, int64, error) {
	ch := make(chan *RootScanResult)
	var wg sync.WaitGroup

	wg.Add(1)
	go fs.recurseDirectories(rpath+fs.config.Path, ch, &wg, false)
	go func() {
		wg.Wait()
		close(ch)
	}()

	count := 0
	size := 0
	for a := range ch {
		count = count + a.Count
		size = size + a.Size
	}

	return count, int64(size), nil
}

func (fs *MantaFs) recurseDirectories(path string, result chan *RootScanResult, wg *sync.WaitGroup, walk bool) {
	defer wg.Done()
	ctx := context.Background()
	out, err := fs.svc.Dir().List(ctx, &storage.ListDirectoryInput{
		DirectoryName: path,
	})
	if err != nil {
		fmt.Printf("could not find %q\n", path)
		return
	}
	metric.MantaListObjectsCompleted(err)
	if out.ResultSetSize == 0 {
		return
	}

	for _, e := range out.Entries {
		np := path + "/" + e.Name
		if e.Type == "directory" {
			wg.Add(1)
			go fs.recurseDirectories(np, result, wg, walk)
		}
		if e.Type == "object" && !walk {
			result <- &RootScanResult{
				Size:  int(e.Size),
				Count: 0,
			}
		}
		if walk {
			result <- &RootScanResult{
				Path:  np,
				Entry: e,
			}

		}
	}
	if !walk {
		result <- &RootScanResult{
			Count: int(out.ResultSetSize),
		}
	}

	return
}

type RootScanResult struct {
	Size  int
	Count int
	Entry *storage.DirectoryEntry
	Path  string
}

// GetDirSize returns the number of files and the size for a folder
// including any subfolders
func (fs *MantaFs) GetDirSize(dirname string) (int, int64, error) {
	return fs.getFileCounts()
}

// GetAtomicUploadPath returns the path to use for an atomic upload.
// Manta uploads are already atomic, we never call this method for Manta
func (*MantaFs) GetAtomicUploadPath(name string) string {
	return ""
}

// GetRelativePath returns the path for a file relative to the user's home dir.
// This is the path as seen by SFTPGo users
func (fs *MantaFs) GetRelativePath(name string) string {
	clean := path.Clean(name)
	removePath := rpath + fs.config.Path
	rel := strings.TrimPrefix(clean, removePath)
	return rel
}

// Walk walks the file tree rooted at root, calling walkFn for each file or
// directory in the tree, including root. The result are unordered
func (fs *MantaFs) Walk(root string, walkFn filepath.WalkFunc) error {
	ch := make(chan *RootScanResult)
	var wg sync.WaitGroup

	wg.Add(1)
	go fs.recurseDirectories(rpath+fs.config.Path, ch, &wg, true)
	go func() {
		wg.Wait()
		close(ch)
	}()

	for a := range ch {
		isDir := false
		if a.Entry.Type == "directory" {
			isDir = true

		}
		walkFn(a.Path, NewFileInfo(a.Entry.Name, isDir, int64(a.Entry.Size), a.Entry.ModifiedTime, false), nil)
	}

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
	if fs.mountPath != "" {
		virtualPath = strings.TrimPrefix(virtualPath, fs.mountPath)
	}
	if !path.IsAbs(virtualPath) {
		virtualPath = path.Clean("/" + virtualPath)
	}
	return fs.Join(rpath, fs.config.Path, fs.config.Prefix, virtualPath), nil
}

// GetMimeType returns the content type
func (fs *MantaFs) GetMimeType(name string) (string, error) {
	info, err := fs.headObject(name)
	if err != nil {
		return "", err
	}
	return info.ContentType, nil
}

// Close closes the fs
func (*MantaFs) Close() error {
	return nil
}

// GetAvailableDiskSize return the available size for the specified path
func (*MantaFs) GetAvailableDiskSize(dirName string) (*sftp.StatVFS, error) {
	return nil, ErrStorageSizeUnavailable
}

func NewMantaClient(config *MantaFsConfig) (*storage.StorageClient, error) {
	accountName := config.Account
	if !config.PrivateKey.IsEmpty() {
		if err := config.PrivateKey.TryDecrypt(); err != nil {
			return nil, err
		}
	}

	keyID := config.KeyId
	userName := config.User
	userName = config.User

	var signer authentication.Signer
	var err error

	var keyBytes []byte
	if config.PrivateKey.GetPayload() != "" {
		keyBytes = []byte(config.PrivateKey.GetPayload())
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

	cfg := &triton.ClientConfig{
		MantaURL:    config.URL,
		AccountName: accountName,
		Username:    "",
		Signers:     []authentication.Signer{signer},
	}

	c, err := storage.NewClient(cfg)
	if err != nil {
		log.Fatalf("storage.NewClient: %s", err)
	}

	return c, nil
}
