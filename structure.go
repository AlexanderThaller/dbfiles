package dbfiles

import (
	"os"

	"github.com/juju/errgo"
)

type Structure interface {
	Create(string) error
	File(string, Driver, []string) (*os.File, error)
}

type Folders struct{}

func (str Folders) Create(basedir string) error {
	return errgo.New("not implemented")
}

func (str Folders) File(basedir string, driver Driver, key []string) (*os.File, error) {
	return nil, errgo.New("not implemented")
}
