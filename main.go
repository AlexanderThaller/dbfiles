package dbfiles

import (
	"os"
	"path/filepath"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errgo"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

type DBFiles struct {
	BaseDir string
	Driver
	Structure
	keys     [][]string
	keysmux  *sync.RWMutex
	filesmux map[string]*sync.RWMutex
}

const DefaultBaseDir = "data"

func New() *DBFiles {
	db := new(DBFiles)
	db.BaseDir = DefaultBaseDir
	db.Driver = CSV{}
	db.Structure = Folders{}
	db.keysmux = new(sync.RWMutex)
	db.filesmux = make(map[string]*sync.RWMutex)

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
	defer file.Close()

	err = db.Driver.Write(file, values)
	if err != nil {
		return errgo.Notef(err, "can not write values")
	}

	file.Close()

	return nil
}

func (db DBFiles) Get(key ...string) ([][]string, error) {
	file, err := db.Structure.File(db.BaseDir, db.Driver, key)
	if err != nil {
		return nil, errgo.Notef(err, "can not open file")
	}
	defer file.Close()

	values, err := db.Driver.Read(file)
	if err != nil {
		return nil, errgo.Notef(err, "can not read values")
	}

	return values, nil
}

func (db DBFiles) Keys() ([][]string, error) {
	err := filepath.Walk(db.BaseDir, db.walkPopulateKeys)
	if err != nil {
		return nil, errgo.Notef(err, "can not walk through basedir")
	}

	return db.keys, nil
}

func (db *DBFiles) walkPopulateKeys(path string, info os.FileInfo, err error) error {
	if info.IsDir() {
		return nil
	}

	// Remove basedir from path
	relpath, err := filepath.Rel(db.BaseDir, path)
	if err != nil {
		return errgo.Notef(err, "can not get relative path")
	}

	// Get driver extention
	driverext := filepath.Ext(relpath)

	// remove driverextention
	nodriverpath := strings.TrimRight(relpath, driverext)

	// Split by path sepperator
	split := strings.Split(nodriverpath, string(os.PathSeparator))

	// Append new key to the db.keys
	db.keysmux.Lock()
	db.keys = append(db.keys, split)
	db.keysmux.Unlock()

	return nil
}

func (db *DBFiles) Destroy() error {
	err := os.RemoveAll(db.BaseDir)
	if err != nil {
		return errgo.Notef(err, "can not remove basedir")
	}

	return nil
}
