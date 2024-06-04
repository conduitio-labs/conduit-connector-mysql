package mysql_test

import (
	"context"
	"testing"

	mysql "github.com/conduitio-labs/conduit-connector-mysql"
	"github.com/matryer/is"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := mysql.NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
