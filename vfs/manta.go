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
	"time"

	"github.com/eikenb/pipeat"
	"github.com/pkg/sftp"

	"github.com/drakkan/sftpgo/v2/logger"
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
	fmt.Println("IN_STAT: " + name)
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

	fmt.Println("RAWR", info)

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

	return info, nil
}

// Lstat returns a FileInfo describing the named file
func (fs *MantaFs) Lstat(name string) (os.FileInfo, error) {
	fmt.Println("IN_LSTAT: " + name)
	return fs.Stat(name)
}

// Open opens the named file for reading
func (fs *MantaFs) Open(name string, offset int64) (File, *pipeat.PipeReaderAt, func(), error) {
	fmt.Println("INSIDE Open")
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
		//metric.AZTransferCompleted(n, 1, err)
	}()

	return nil, r, cancelFn, nil
}

// Create creates or opens the named file for writing
func (fs *MantaFs) Create(name string, flag int) (File, *PipeWriter, func(), error) {
	fmt.Println("INSIDE Create: "+name, flag)
	r, w, err := pipeat.PipeInDir(fs.localTempDir)
	if err != nil {
		return nil, nil, nil, err
	}
	p := NewPipeWriter(w)

	ctx, cancelFn := context.WithCancel(context.Background())
	ct := mime.TypeByExtension(path.Ext(name))

	fmt.Println("tooter", ct)

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
		//metric.AZTransferCompleted(r.GetReadedBytes(), 0, err)
	}()

	return nil, p, cancelFn, nil
}

func (fs *MantaFs) Rename(source, target string) error {
	fmt.Println("INSIDE Rename")
	return nil
}

// Remove removes the named file or (empty) directory.
func (fs *MantaFs) Remove(name string, isDir bool) error {
	fmt.Println("INSIDE Remove")
	ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(fs.ctxTimeout))
	defer cancelFn()
	err := fs.svc.Dir().Delete(ctx, &storage.DeleteDirectoryInput{
		DirectoryName: name,
	})
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
	fmt.Println("INSIDE ReadDir")
	fmt.Println("READ_DIR", dirname)
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

	return result, nil
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
	fmt.Println("INSIDE_IsNotExist")
	if err == nil {
		return false
	}
	fmt.Println(err.Error())

	return strings.Contains(err.Error(), "404")

}

// IsPermission returns a boolean indicating whether the error is known to
// report that permission is denied.
func (*MantaFs) IsPermission(err error) bool {
	fmt.Println("INSIDE_IsPermission")
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "403")
}

// IsNotSupported returns true if the error indicate an unsupported operation
func (*MantaFs) IsNotSupported(err error) bool {
	fmt.Println("INSIDE_IsNotSupported")
	if err == nil {
		return false
	}
	return err == ErrVfsUnsupported
}

// CheckRootPath creates the specified local root directory if it does not exists
func (fs *MantaFs) CheckRootPath(username string, uid int, gid int) bool {
	osFs := NewOsFs(fs.ConnectionID(), fs.localTempDir, "")
	// Create Root Path on Manta
	fs.Mkdir(rpath + fs.config.Path)
	// we need a local directory for temporary files
	return osFs.CheckRootPath(username, uid, gid)
}

// ScanRootDirContents returns the number of files contained in the bucket,
// and their size
func (fs *MantaFs) ScanRootDirContents() (int, int64, error) {
	fmt.Println("INSIDE_ScanRootDirContents")
	return fs.getFileCounts()
}

func (fs *MantaFs) getFileCounts() (int, int64, error) {
	ch := make(chan *RootScanResult)
	var wg sync.WaitGroup

	// First call to Factorial recursive function
	wg.Add(1)
	go fs.recurseDirectories(rpath + fs.config.Path, ch, &wg)
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

	fmt.Printf("Count: %d, Size: %d\n", count, size)
	return count, int64(size), nil
}

func (fs *MantaFs) recurseDirectories(path string, result chan *RootScanResult, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()
	out, err := fs.svc.Dir().List(ctx, &storage.ListDirectoryInput{
		DirectoryName: path,
	})
	if err != nil {
		fmt.Printf("could not find %q\n", path)
		return
	}

	if out.ResultSetSize == 0 {
		return
	}

	for _, e := range out.Entries {
		if e.Type == "directory" {
			np := path + "/" + e.Name
			wg.Add(1)
			go fs.recurseDirectories(np, result, wg)
		}
		if e.Type == "object" {
			result <- &RootScanResult{
				Size:  int(e.Size),
				Count: 0,
			}
		}
	}

	result <- &RootScanResult{
		Size:  0,
		Count: int(out.ResultSetSize),
	}

	return
}

type RootScanResult struct {
	Size  int
	Count int
}

// GetDirSize returns the number of files and the size for a folder
// including any subfolders
func (fs *MantaFs) GetDirSize(dirname string) (int, int64, error) {
	fmt.Println("INSIDE_GET_DIR_SIZE")
	return fs.getFileCounts()
}

// GetAtomicUploadPath returns the path to use for an atomic upload.
// S3 uploads are already atomic, we never call this method for Manta
func (*MantaFs) GetAtomicUploadPath(name string) string {
	return ""
}

// GetRelativePath returns the path for a file relative to the user's home dir.
// This is the path as seen by SFTPGo users
func (fs *MantaFs) GetRelativePath(name string) string {
	fmt.Println("INSIDE_GET_RELATIVE_PATH: " + name)
	return "/rawr"
}

// Walk walks the file tree rooted at root, calling walkFn for each file or
// directory in the tree, including root. The result are unordered
func (fs *MantaFs) Walk(root string, walkFn filepath.WalkFunc) error {
	fmt.Println("INSIDE WALK")
	ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(fs.ctxTimeout))
	defer cancelFn()
	dirList, err := fs.svc.Dir().List(ctx, &storage.ListDirectoryInput{
		DirectoryName: fs.config.Path,
	})
	if err != nil {
		return err
	}

	fmt.Println(dirList)
	return nil
}

// Join joins any number of path elements into a single path
func (*MantaFs) Join(elem ...string) string {
	fmt.Println("INSIDE_JOIN")
	return path.Join(elem...)
}

// HasVirtualFolders returns true if folders are emulated
func (*MantaFs) HasVirtualFolders() bool {
	fmt.Println("INSIDE_HAS_VIRT_FOLDERS")
	return true
}

// ResolvePath returns the matching filesystem path for the specified virtual path
func (fs *MantaFs) ResolvePath(virtualPath string) (string, error) {
	fmt.Println("INSIDE_RESOLVE_PATH: ", virtualPath)
	if fs.mountPath != "" {
		virtualPath = strings.TrimPrefix(virtualPath, fs.mountPath)
	}
	if !path.IsAbs(virtualPath) {
		virtualPath = path.Clean("/" + virtualPath)
	}
	fmt.Println(fs.Join("/", fs.config.KeyPrefix, virtualPath))
	return fs.Join(rpath, fs.config.Path, fs.config.KeyPrefix, virtualPath), nil
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
	fmt.Println("INSIDE_CLOSE")
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
