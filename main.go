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
	keys       [][]string
	keysmux    *sync.RWMutex
	WriteQueue chan (record)
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
	db.keysmux = new(sync.RWMutex)
	db.WriteQueue = make(chan (record), 10000)

	go db.runQueue()
	go db.runQueue()
	go db.runQueue()
	go db.runQueue()
	go db.runQueue()
	go db.runQueue()
	go db.runQueue()

	return db
}

func (db DBFiles) runQueue() {
	for {
		record := <-db.WriteQueue
		log.Debug("new record: ", record)
		log.Debug("Basedir: ", record.basedir)

		_, err := os.Stat(record.basedir)
		if os.IsNotExist(err) {
			err := db.Structure.Create(record.basedir)
			if err != nil {
				record.errorChan <- errgo.Notef(err, "can not create structure")
				continue
			}
		}

		file, err := db.Structure.File(record.basedir, db.Driver, record.key)
		if err != nil {
			record.errorChan <- errgo.Notef(err, "can not open file")
			continue
		}

		err = db.Driver.Write(file, record.values)
		if err != nil {
			record.errorChan <- errgo.Notef(err, "can not write values")
			continue
		}

		var data []byte
		io.ReadFull(file, data)
		log.Debug("Data: ", string(data))

		record.errorChan <- nil
		log.Debug("finished writing record: ", record)
	}
}

func (db *DBFiles) Put(values []string, key ...string) error {
	errorChan := make(chan (error))

	rec := record{
		values:    values,
		key:       key,
		errorChan: errorChan,
		basedir:   db.BaseDir,
	}

	db.WriteQueue <- rec

	err := <-errorChan

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
