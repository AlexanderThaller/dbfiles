package dbfiles

import (
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/juju/errgo"
)

type Structure interface {
	Create(string) error
	File(string, Driver, []string) (*os.File, error)
}

type Folders struct{}

func (str Folders) Create(basedir string) error {
	err := os.MkdirAll(basedir, 0755)
	if err != nil {
		return errgo.Notef(err, "can not create basedir")
	}

	return nil
}

func (str Folders) File(basedir string, driver Driver, key []string) (*os.File, error) {
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
