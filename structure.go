package dbfiles

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/juju/errgo"
)

type Structure interface {
	Create(string) error
	File(string, Driver, []string) (io.ReadWriteCloser, error)
}

type Folders struct {
}

func NewFolders() Folders {
	return Folders{}
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

	file, err := os.OpenFile(keypath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0640)
	if err != nil {
		return nil, errgo.Notef(err, "can not open file")
	}

	return file, nil
}

func NewFlat() Flat {
	return Flat{}
}

type Flat struct {
}

func (str Flat) Create(basedir string) error {
	err := os.MkdirAll(basedir, 0755)
	if err != nil {
		return errgo.Notef(err, "can not create basedir")
	}

	return nil
}

func (str Flat) File(basedir string, driver Driver, key []string) (io.ReadWriteCloser, error) {
	keypath := path.Join(basedir, strings.Join(key, ".")) + "." + driver.Extention()

	folderpath := filepath.Dir(keypath)

	err := os.MkdirAll(folderpath, 0755)
	if err != nil {
		return nil, errgo.Notef(err, "can not create keypath")
	}

	file, err := os.OpenFile(keypath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0640)
	if err != nil {
		return nil, errgo.Notef(err, "can not open file")
	}

	return file, nil
}
