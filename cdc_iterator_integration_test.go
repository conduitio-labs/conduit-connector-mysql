package mysql

import (
	"testing"

	"github.com/matryer/is"
)

func TestCDCIterator(t *testing.T) {
	is := is.New(t)

	db := createTestConnection(is)

	// implement creating the cdc iterator here.
	panic("unimplemented")

	testTables.drop(is, db)
	testTables.create(is, db)
	testTables.insertData(is, db)
}
