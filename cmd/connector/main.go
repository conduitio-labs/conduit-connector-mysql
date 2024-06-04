package main

import (
	mysql "github.com/conduitio-labs/conduit-connector-mysql"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	sdk.Serve(mysql.Connector)
}
