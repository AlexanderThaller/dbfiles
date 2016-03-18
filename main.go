package dbfiles

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errgo"
)

func init() {
	//	log.SetLevel(log.DebugLevel)
}

type DBFiles struct {
	BaseDir string
	Driver
	Structure
	WriteQueue chan (record)

	keysmux *sync.RWMutex
	keys    [][]string
}

type record struct {
	values    []string
	key       []string
	errorChan chan (error)
	basedir   string
}

const DefaultBaseDir = "data"

func New() *DBFiles {
	db := new(DBFiles)
	db.BaseDir = DefaultBaseDir
	db.Driver = CSV{}
	db.Structure = NewFolders()

	db.WriteQueue = make(chan (record), 10000)
	db.keysmux = new(sync.RWMutex)

	return db
}

func (db *DBFiles) Put(values []string, key ...string) error {
	record := record{
		values:  values,
		key:     key,
		basedir: db.BaseDir,
	}

	_, err := os.Stat(record.basedir)
	if os.IsNotExist(err) {
		err := db.Structure.Create(record.basedir)
		if err != nil {
			return errgo.Notef(err, "can not create structure")

		}
	}

	file, err := db.Structure.File(record.basedir, db.Driver, record.key)
	if err != nil {
		return errgo.Notef(err, "can not open file")
	}
	defer file.Close()

	err = db.Driver.Write(file, record.values)
	if err != nil {
		return errgo.Notef(err, "can not write values")
	}

	var data []byte
	io.ReadFull(file, data)
	log.Debug("Data: ", string(data))

	log.Debug("finished writing record: ", record)

	return err
}

func (db DBFiles) Get(key ...string) ([][]string, error) {
	file, err := db.Structure.File(db.BaseDir, db.Driver, key)
	if err != nil {
		return nil, errgo.Notef(err, "can not open file")
	}

	values, err := db.Driver.Read(file)
	if err != nil {
		return nil, errgo.Notef(err, "can not read values")
	}

	return values, nil
}

func (db DBFiles) Keys() ([][]string, error) {
	_, err := os.Stat(db.BaseDir)
	if os.IsNotExist(err) {
		return [][]string{}, nil
	}

	err = filepath.Walk(db.BaseDir, db.walkPopulateKeys)
	if err != nil {
		return nil, errgo.Notef(err, "can not walk through basedir")
	}

	return db.keys, nil
}

func (db *DBFiles) walkPopulateKeys(path string, info os.FileInfo, err error) error {
	if err != nil {
		return errgo.Notef(err, "error is not empty")
	}

	if info == nil {
		return errgo.New("directory info is empty")
	}

	//Skip git folder
	if info.IsDir() && info.Name() == ".git" {
		return filepath.SkipDir
	}

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
	nodriverpath := relpath[0 : len(relpath)-len(driverext)]

	// Split by path sepperator
	split := strings.Split(nodriverpath, string(os.PathSeparator))

	// Append new key to the db.keys
	db.keysmux.Lock()
	db.keys = append(db.keys, split)
	db.keysmux.Unlock()

	log.Debug("Path: ", path)
	log.Debug("driverext: ", driverext)
	log.Debug("Nodriverpath: ", nodriverpath)
	log.Debug("Split: ", split)

	return nil
}

func (db *DBFiles) Destroy() error {
	err := os.RemoveAll(db.BaseDir)
	if err != nil {
		return errgo.Notef(err, "can not remove basedir")
	}

	return nil
}
