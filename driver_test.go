package dbfiles

import (
	"errors"
	"testing"
)

func Test_Write_Fail_FailingWriter(t *testing.T) {
	csv := new(CSV)
	writer := new(FailWriter)

	err := csv.Write(writer, []string{})
	if err == nil {
		t.Logf("err: ", err)
		t.Fatal("error should not be nil")
	}
}

func Test_Read_Fail_FailingReader(t *testing.T) {
	csv := new(CSV)
	reader := new(FailReader)

	_, err := csv.Read(reader)
	if err == nil {
		t.Logf("err: ", err)
		t.Fatal("error should not be nil")
	}
}

type FailWriter struct{}

func (writer FailWriter) Write(p []byte) (n int, err error) {
	return 0, errors.New("this should always fail")
}

type FailReader struct{}

func (reader FailReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("this should always fail")
}
