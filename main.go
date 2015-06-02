package dbfiles

import (
	"io"

	"github.com/juju/errgo"
)

type DBFiles struct {
	BaseDir string
	Driver
	Structure
}

const DefaultBaseDir = "data"
const DefaultDriver = CSV

func New() *DBFiles {
	db := new(DBFiles)
	db.BaseDir = DefaultBaseDir
	db.Driver = DefaultDriver
	db.Structure = Folders{}

	return db
}

func (db DBFiles) Put(values []string, key ...string) error {
	err := db.Structure.Create(db.BaseDir)
	if err != nil {
		return errgo.Notef(err, "can not create structure")
	}

	file, err := db.Structure.File(db.BaseDir, db.Driver, key)
	if err != nil {
		return errgo.Notef(err, "can not open file")
	}

	err = writeData(file, db.Driver)
	if err != nil {
		return errgo.Notef(err, "can not write to file")
	}

	return nil
}

func (db DBFiles) Get(key ...string) ([][]string, error) {
	return nil, errgo.New("not implemented")
}

func writeData(writer io.Writer, driver Driver) error {
	return errgo.New("not implemented")
}
