package mysql

import (
	"context"
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func testCombinedIterator(ctx context.Context, is *is.I) (Iterator, func()) {
	iterator, err := newCombinedIterator(ctx, combinedIteratorConfig{
		snapshotConfig: snapshotIteratorConfig{
			tableKeys: testTableKeys(),
			db:        testutils.TestConnection(is),
			database:  "meroxadb",
			tables:    testTables.Tables(),
		},
		cdcConfig: cdcIteratorConfig{
			SourceConfig: SourceConfig{
				Config: common.Config{
					Host:     "127.0.0.1",
					Port:     3306,
					User:     "root",
					Password: "meroxaadmin",
					Database: "meroxadb",
				},
				Tables: []string{"users"},
			},
			tableKeys: testTableKeys(),
		},
	})
	is.NoErr(err)

	return iterator, func() { is.NoErr(iterator.Teardown(ctx)) }
}

func TestCombinedIterator(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.TestConnection(is)

	testTables.Drop(is, db)
	testTables.Create(is, db)

	user1 := testTables.InsertUser(is, db, "user1")
	user2 := testTables.InsertUser(is, db, "user2")
	user3 := testTables.InsertUser(is, db, "user3")

	iterator, cleanup := testCombinedIterator(ctx, is)
	defer cleanup()

	user1Updated := user1.WithName("user1-updated")
	user2Updated := user2.WithName("user2-updated")
	user3Updated := user3.WithName("user3-updated")

	testTables.UpdateUser(is, db, user1Updated)
	testTables.UpdateUser(is, db, user2Updated)
	testTables.UpdateUser(is, db, user3Updated)

	for range []testutils.User{user1, user2, user3} {
		rec, err := iterator.Next(ctx)
		is.NoErr(err)
		is.NoErr(iterator.Ack(ctx, rec.Position))

		is.Equal(rec.Operation, sdk.OperationSnapshot)
	}

	for range [][]testutils.User{
		{user1, user1Updated},
		{user2, user2Updated},
		{user3, user3Updated},
	} {
		rec, err := iterator.Next(ctx)
		is.NoErr(err)
		is.NoErr(iterator.Ack(ctx, rec.Position))

		is.Equal(rec.Operation, sdk.OperationUpdate)
	}
}
