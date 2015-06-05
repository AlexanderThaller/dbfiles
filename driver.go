package dbfiles

import (
	"encoding/csv"
	"io"

	"github.com/juju/errgo"
)

type Driver interface {
	Extention() string
	Write(io.Writer, []string) error
	Read(io.Reader) ([][]string, error)
}

type CSV struct{}

func (driver CSV) Extention() string {
	return "csv"
}

func (driver CSV) Write(writer io.Writer, values []string) error {
	csvwriter := csv.NewWriter(writer)

	err := csvwriter.Write(values)
	if err != nil {
		return errgo.Notef(err, "can not write to csv writer")
	}
	csvwriter.Flush()

	return nil
}

func (driver CSV) Read(reader io.Reader) ([][]string, error) {
	csvreader := csv.NewReader(reader)

	var values [][]string

	for {
		value, err := csvreader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, errgo.Notef(err, "can not read value")
		}

		values = append(values, value)
	}

	return values, nil
}
