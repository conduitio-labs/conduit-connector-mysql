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
			db:        testutils.Connection(is),
			database:  "meroxadb",
			tables:    []string{"users"},
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
	db := testutils.Connection(is)

	userTable.Recreate(is, db)

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")

	iterator, cleanup := testCombinedIterator(ctx, is)
	defer cleanup()

	user1Updated := userTable.Update(is, db, user1.Update())
	user2Updated := userTable.Update(is, db, user2.Update())
	user3Updated := userTable.Update(is, db, user3.Update())

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
