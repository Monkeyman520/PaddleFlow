package ufs

import "C"

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Monkeyman520/gogfapi/gfapi"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

var _ FileHandle = &glusterFileHandle{}

type glusterFileHandle struct {
	File   *gfapi.File
}

func (fh *glusterFileHandle) Read(dest []byte, off uint64) (int, error) {
	return fh.File.ReadAt(dest, int64(off))
}

func (fh *glusterFileHandle) Write(data []byte, off uint64) (written uint32, code error) {
	wroteLen, code := fh.File.WriteAt(data, int64(off))
	written = uint32(wroteLen)
	return
}

// Flush is called for close() call on a file descriptor. In
// case of duplicated descriptor, it may be called more than
// once for a file.
func (fh *glusterFileHandle) Flush() error {
	return nil
}

// This is called to before the file handle is forgotten. This
// method has no return value, so nothing can synchronizes on
// the call. Any cleanup that requires specific synchronization or
// could fail with I/O errors should happen in Flush instead.
func (fh *glusterFileHandle) Release() {
	fh.File.Close()
	return
}

func (fh *glusterFileHandle) Fsync(flags int) error {
	return fh.File.Fsync()
}

// The methods below may be called on closed files, due to
// concurrency.  In that case, you should return EBADF.
func (fh *glusterFileHandle) Truncate(size uint64) error {
	return fh.File.Truncate(int64(size))
}

func (fh *glusterFileHandle) Allocate(off uint64, size uint64, mode uint32) error {
	return fh.File.Fallocate(int(mode), int64(off), int64(size))
}

var _ UnderFileStorage = &glusterFileSystem{}

type glusterFileSystem struct {
	volume *gfapi.Volume
	sync.Mutex
}

func NewGlusterFileSystem(properties map[string]interface{}) (UnderFileStorage, error) {
	fs := &glusterFileSystem{
		volume: &gfapi.Volume{},
	}

	volumeName, host := properties[common.Volume].(string), properties[common.Address].(string)
	hosts := strings.Split(host, ",")
	if len(volumeName) == 0 || len(hosts) == 0 {
		return nil, fmt.Errorf("volumeName[%s] host[%s] is invalid", volumeName, host)
	}

	err := fs.volume.Init(volumeName, hosts...)
	if err != nil {
		return nil, err
	}

	runtime.SetFinalizer(fs, func(fs *glusterFileSystem) {
		fs.volume.Unmount()
	})
	return fs, nil
}

// Used for pretty printing.
func (fs *glusterFileSystem) String() string {
	return common.GlusterFSType
}

// Attributes.  This function is the main entry point, through
// which FUSE discovers which files and directories exist.
//
// If the filesystem wants to implement hard-links, it should
// return consistent non-zero FileInfo.Ino data.  Using
// hardlinks incurs a performance hit.
func (fs *glusterFileSystem) GetAttr(name string) (*base.FileInfo, error) {
	fs.Lock()
	defer fs.Unlock()

	info, err := fs.volume.Stat(name)
	if err != nil {
		return nil, err
	}

	owner, group := utils.GetOwnerGroup(info)

	f := &base.FileInfo{
		Name:  name,
		Path:  name,
		Size:  info.Size(),
		IsDir: info.IsDir(),
		Mtime: uint64(info.ModTime().Unix()),
		Owner: owner,
		Group: group,
		Mode:  info.Mode(),
		Sys:   info.Sys(),
	}

	return f, nil
}

// These should update the file's ctime too.
// Note: raw FUSE setattr is translated into Chmod/Chown/Utimens in the higher level APIs.
func (fs *glusterFileSystem) Chmod(name string, mode uint32) error {
	fs.Lock()
	defer fs.Unlock()

	return fs.volume.Chmod(name, os.FileMode(mode))
}

func (fs *glusterFileSystem) Chown(name string, uid uint32, gid uint32) error {
	fs.Lock()
	defer fs.Unlock()

	return fs.volume.Chown(name, int(uid), int(gid))
}

func (fs *glusterFileSystem) Utimens(name string, Atime *time.Time, Mtime *time.Time) error {
	fs.Lock()
	defer fs.Unlock()

	return fs.volume.Chtimes(name, *Atime, *Mtime)
}

func (fs *glusterFileSystem) Truncate(name string, size uint64) error {
	fs.Lock()
	defer fs.Unlock()

	return fs.volume.Truncate(name, int64(size))
}

func (fs *glusterFileSystem) Access(name string, mode uint32, callerUid uint32, callerGid uint32) error {
	return nil
}

// Tree structure
func (fs *glusterFileSystem) Link(oldName string, newName string) error {
	return fs.volume.Link(oldName, newName)
}

func (fs *glusterFileSystem) Mkdir(name string, mode uint32) error {
	fs.Lock()
	defer fs.Unlock()

	return fs.volume.Mkdir(name, os.FileMode(mode))
}

func (fs *glusterFileSystem) Mknod(name string, mode uint32, dev uint32) error {
	return syscall.ENOSYS
}

func (fs *glusterFileSystem) Rename(oldName string, newName string) error {
	fs.Lock()
	defer fs.Unlock()

	return fs.volume.Rename(oldName, newName)
}

func (fs *glusterFileSystem) Rmdir(name string) error {
	fs.Lock()
	defer fs.Unlock()

	return fs.volume.Rmdir(name)
}

func (fs *glusterFileSystem) Unlink(name string) error {
	fs.Lock()
	defer fs.Unlock()

	return fs.volume.Unlink(name)
}

// Extended attributes.
func (fs *glusterFileSystem) GetXAttr(name string, attribute string) (data []byte, err error) {
	fs.Lock()
	defer fs.Unlock()

	_, err = fs.volume.Getxattr(name, attribute, data)
	return data, err
}

func (fs *glusterFileSystem) ListXAttr(name string) (attributes []string, err error) {
	return
}

func (fs *glusterFileSystem) RemoveXAttr(name string, attr string) error {
	fs.Lock()
	defer fs.Unlock()

	return fs.volume.Removexattr(name, attr)
}

func (fs *glusterFileSystem) SetXAttr(name string, attr string, data []byte, flags int) error {
	fs.Lock()
	defer fs.Unlock()

	return fs.volume.Setxattr(name, attr, data, flags)
}

// File handling.  If opening for writing, the file's mtime
// should be updated too.
func (fs *glusterFileSystem) Open(name string, flags uint32, size uint64) (FileHandle, error) {
	fs.Lock()
	defer fs.Unlock()

	handle, err := fs.volume.OpenFile(name, int(flags), os.FileMode(size))
	return &glusterFileHandle{
		File:   handle,
	}, err
}

func (fs *glusterFileSystem) Create(name string, flags uint32, mode uint32) (FileHandle, error) {
	handle, err := fs.volume.Create(name, int(flags), os.FileMode(mode))
	if err != nil {
		return nil, err
	}
	return &glusterFileHandle{
		File:   handle,
	}, nil
}

// Directory handling
func (fs *glusterFileSystem) ReadDir(name string) (stream []DirEntry, err error) {
	file, err := fs.volume.Open(name, syscall.O_RDONLY)
	if err != nil {
		return nil, err
	}
	if file == nil {
		return nil, &os.PathError{}
	}

	output := make([]DirEntry, 0)
	infos, err := file.Readdir(0)
	for i := range infos {
		// workaround for https://err.google.com/p/go/issues/detail?id=5960
		if infos[i] == nil {
			continue
		}
		n := infos[i].Name()
		d := DirEntry{
			Name: n,
		}
		attr := sysToAttr(infos[i])
		d.Attr = &attr
		output = append(output, d)
	}

	return output, nil
}

// Symlinks.
func (fs *glusterFileSystem) Symlink(value string, linkName string) error {
	return fs.volume.Symlink(value, linkName)
}

func (fs *glusterFileSystem) Readlink(name string) (string, error) {
	return fs.volume.Readlink(name)
}

func (fs *glusterFileSystem) StatFs(name string) *base.StatfsOut {
	statVfs, err := fs.volume.Statvfs(name)
	if err != nil {
		return &base.StatfsOut{}
	}
	return &base.StatfsOut{
		Blocks:  uint64(statVfs.Blocks),
		Bfree:   uint64(statVfs.Bfree),
		Bavail:  uint64(statVfs.Bavail),
		Files:   uint64(statVfs.Files),
		Ffree:   uint64(statVfs.Ffree),
		Bsize:   uint32(statVfs.Bsize),
		NameLen: uint32(statVfs.Namemax),
		Frsize:  uint32(statVfs.Frsize),
	}
}

func (fs *glusterFileSystem) Get(name string, flags uint32, off int64, limit int64) (io.ReadCloser, error) {
	return nil, nil
}

func (fs *glusterFileSystem) Put(name string, reader io.Reader) error {
	return nil
}

func init() {
	RegisterUFS(common.GlusterFSType, NewGlusterFileSystem)
}
