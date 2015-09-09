package dbfiles

import (
	"io"

	"github.com/AlexanderThaller/dbfiles/csv"

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
	csvreader.VariableFieldsPerRecord = true

	var values [][]string

	values, err := csvreader.ReadAll()
	if err != nil {
		return nil, errgo.Notef(err, "can not read all records from file")
	}

	return values, nil
}
