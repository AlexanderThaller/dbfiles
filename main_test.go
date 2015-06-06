package dbfiles

import (
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/juju/errgo"
)

const longvalue = `test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test
  test test test test test test test test test test test test test test test`

func tmp_db() (*DBFiles, error) {
	db := New()

	tmppath := "/tmp/dbfiles"
	err := os.MkdirAll(tmppath, 0755)
	if err != nil {
		return nil, errgo.Notef(err, "can not create tmpdir path")
	}

	tmpdir, err := ioutil.TempDir(tmppath, "main_test")
	if err != nil {
		return nil, errgo.Notef(err, "can not open tmpdir")
	}
	db.BaseDir = tmpdir

	return db, nil
}

func Test_DBFiles_Put(t *testing.T) {
	db, err := tmp_db()
	if err != nil {
		t.Fatalf(errgo.Details(err))
	}

	err = db.Put([]string{"test"}, "Put")
	if err != nil {
		t.Fatalf(errgo.Details(err))
	}

	db.Destroy()
}

func Test_DBFiles_PutMultiValue(t *testing.T) {
	db, err := tmp_db()
	if err != nil {
		t.Fatalf(errgo.Details(err))
	}

	err = db.Put([]string{
		"test1",
		"test2",
		"test3",
		"test4",
		"test5",
		"test6",
	}, "PutMultiValue")
	if err != nil {
		t.Fatalf(errgo.Details(err))
	}

	db.Destroy()
}

func Test_DBFiles_PutParallel(t *testing.T) {
	db, err := tmp_db()
	if err != nil {
		t.Fatalf(errgo.Details(err))
	}

	wg := new(sync.WaitGroup)

	for i := 0; i != 100000; i++ {
		wg.Add(1)

		counter := i
		go func() {
			defer wg.Done()

			value := longvalue + strconv.Itoa(counter)

			err = db.Put([]string{value}, "PutParallel")
			if err != nil {
				t.Fatalf(errgo.Details(err))
			}
		}()
	}

	wg.Wait()

	db.Destroy()
}

func Test_DBFiles_Get(t *testing.T) {
	db, err := tmp_db()
	if err != nil {
		t.Fatalf(errgo.Details(err))
	}

	in := [][]string{
		[]string{"test"},
	}

	for _, value := range in {
		err = db.Put(value, "Get")
		if err != nil {
			t.Fatalf(errgo.Details(err))
		}
	}

	out, err := db.Get("Get")
	if err != nil {
		t.Fatalf(errgo.Details(err))
	}

	if !reflect.DeepEqual(in, out) {
		t.Log("in :", in)
		t.Log("out:", out)
		t.Log("datadir:", db.BaseDir)
		t.Fatalf("out is not equal to in")
	}

	db.Destroy()
}

func Test_Keys(t *testing.T) {
	db, err := tmp_db()
	if err != nil {
		t.Fatalf(errgo.Details(err))
	}

	in := [][]string{
		[]string{"test1", "test1", "test1"},
		[]string{"test1", "test1", "test2"},
		[]string{"test1", "test1"},
		[]string{"test1", "test2"},
		[]string{"test1"},
		[]string{"test2"},
	}

	for _, key := range in {
		err = db.Put([]string{"test"}, key...)
		if err != nil {
			t.Fatalf(errgo.Details(err))
		}
	}

	out, err := db.Keys()
	if err != nil {
		t.Fatalf(errgo.Details(err))
	}

	if !reflect.DeepEqual(in, out) {
		t.Log("in :", in)
		t.Log("out:", out)
		t.Log("datadir:", db.BaseDir)
		t.Fatalf("out is not equal to in")
	}

	db.Destroy()
}

func BenchmarkPut(b *testing.B) {
	db, err := tmp_db()
	if err != nil {
		b.Fatalf(errgo.Details(err))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = db.Put([]string{"test"}, "Put")
		if err != nil {
			b.Fatalf(errgo.Details(err))
		}
	}

	db.Destroy()
}

func BenchmarkGet(b *testing.B) {
	db, err := tmp_db()
	if err != nil {
		b.Fatalf(errgo.Details(err))
	}

	err = db.Put([]string{"test"}, "Get")
	if err != nil {
		b.Fatalf(errgo.Details(err))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = db.Get("Get")
		if err != nil {
			b.Fatalf(errgo.Details(err))
		}
	}

	db.Destroy()
}

func BenchmarkKeys(b *testing.B) {
	db, err := tmp_db()
	if err != nil {
		b.Fatalf(errgo.Details(err))
	}

	err = db.Put([]string{"test"}, "Get")
	if err != nil {
		b.Fatalf(errgo.Details(err))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = db.Keys()
		if err != nil {
			b.Fatalf(errgo.Details(err))
		}
	}

	db.Destroy()
}
