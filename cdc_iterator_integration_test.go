package mysql

import (
	"context"
	"testing"

	"github.com/matryer/is"
)

func testCdcIterator(ctx context.Context, is *is.I) Iterator {
	iterator, err := newCdcIterator(ctx, SourceConfig{
		Config: testConfig(),
		Tables: []string{"users"},
	})
	is.NoErr(err)

	return iterator
}

func TestCDCIterator(t *testing.T) {
	return
	ctx := testContext(t)
	is := is.New(t)

	db := createTestConnection(is)

	testTables.drop(is, db)
	testTables.create(is, db)

	iterator := testCdcIterator(ctx, is)

	testTables.insertData(is, db)

	for {
		_, err := iterator.Next(ctx)
		is.NoErr(err)
	}
}
