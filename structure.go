package dbfiles

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errgo"
)

type Structure interface {
	Create(string) error
	File(string, Driver, []string) (io.ReadWriteCloser, error)
}

type Folders struct {
	files    map[string]*FileLock
	filesMux *sync.RWMutex
}

func NewFolders() Folders {
	return Folders{
		filesMux: new(sync.RWMutex),
		files:    make(map[string]*FileLock),
	}
}

func (str Folders) Create(basedir string) error {
	err := os.MkdirAll(basedir, 0755)
	if err != nil {
		return errgo.Notef(err, "can not create basedir")
	}

	return nil
}

func (str Folders) File(basedir string, driver Driver, key []string) (io.ReadWriteCloser, error) {
	keypath := path.Join(basedir, strings.Join(key, "/")) + "." + driver.Extention()

	folderpath := filepath.Dir(keypath)

	err := os.MkdirAll(folderpath, 0755)
	if err != nil {
		return nil, errgo.Notef(err, "can not create keypath")
	}

	return str.newOrExistingFileLock(keypath)
}

func (str Folders) newOrExistingFileLock(path string) (io.ReadWriteCloser, error) {
	log.Debug("checking file ", path)

	str.filesMux.RLock()
	file, exists := str.files[path]
	str.filesMux.RUnlock()

	if exists {
		log.Debug("already open ", path)
		return file, nil
	}

	log.Debug("open new file ", path)
	str.filesMux.Lock()
	file, err := NewFileLock(path)
	if err != nil {
		return nil, errgo.Notef(err, "can not open file for this path")
	}

	str.files[path] = file
	str.filesMux.Unlock()

	return file, nil
}

type FileLock struct {
	mutex *sync.RWMutex
	file  *os.File
}

func NewFileLock(path string) (*FileLock, error) {
	lock := new(FileLock)
	lock.mutex = new(sync.RWMutex)

	var file *os.File
	var err error

	counter := 10
	for {
		file, err = os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0640)
		if err != nil {
			if counter != 0 {
				counter--
				time.Sleep(100 * time.Millisecond)
				continue
			}

			return nil, errgo.Notef(err, "can not open file")
		}

		break
	}

	lock.file = file

	return lock, nil
}

func (fileLock *FileLock) Close() error {
	return nil
}

func (fileLock *FileLock) Read(p []byte) (n int, err error) {
	log.Debug("Running read")
	log.Debug("File: ", &fileLock.file)

	fileLock.mutex.RLock()
	fileLock.file.Seek(0, 0)
	n, err = fileLock.file.Read(p)
	fileLock.file.Seek(2, 0)
	err = io.EOF
	fileLock.mutex.RUnlock()

	log.Debug("Read: ", n)
	log.Debug("Error: ", err)
	log.Debug("Data: ", string(p))

	return
}

func (fileLock *FileLock) Write(p []byte) (n int, err error) {
	log.Debug("Running write")
	log.Debug("File: ", &fileLock.file)

	fileLock.mutex.Lock()
	n, err = fileLock.file.Write(p)
	fileLock.mutex.Unlock()

	log.Debug("Read: ", n)
	log.Debug("Error: ", err)
	log.Debug("Data: ", string(p))

	return
}
