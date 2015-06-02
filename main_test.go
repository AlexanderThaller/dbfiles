package dbfiles

import "testing"

func Test_DBFiles_Put(t *testing.T) {
	db := New()

	err := db.Put([]string{"test"}, "test", "dbfiles", "put")
	if err != nil {
		t.Fatalf(err.Error())
	}
}
