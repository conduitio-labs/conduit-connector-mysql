module github.com/conduitio-labs/conduit-connector-mysql

go 1.26.9

require (
	github.com/Masterminds/squirrel v1.5.4
	github.com/conduitio/conduit-commons v0.10.0
	github.com/conduitio/conduit-connector-sdk v0.17.0
	github.com/go-mysql-org/go-mysql v1.14.0
	github.com/go-sql-driver/mysql v1.10.1
	github.com/google/go-cmp v0.7.0
	github.com/iskorotkov/avro/v2 v2.34.0
	github.com/jmoiron/sqlx v1.4.0
	github.com/matryer/is v1.4.1
	github.com/rs/zerolog v1.35.1
	go.uber.org/goleak v1.3.0
	gopkg.in/tomb.v2 v2.0.0-20161208151619-d5d1b5820637
	gorm.io/driver/mysql v1.6.0
	gorm.io/gorm v1.31.2
	pgregory.net/rapid v1.3.0
)

require (
	dario.cat/mergo v1.0.1 // indirect
	github.com/conduitio/evolviconf v0.1.1 // indirect
	github.com/conduitio/evolviconf/evolviyaml v0.1.0 // indirect
	github.com/conduitio/yaml/v3 v3.3.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pingcap/errors v0.11.5-0.20250523034308-74f78ae071ee // indirect
	github.com/pingcap/failpoint v0.0.0-20251231045439-91d91e123837 // indirect
	github.com/pingcap/log v1.1.1-0.20241212030209-7e3ff8601a2a // indirect
	github.com/pingcap/tidb/pkg/parser v0.0.0-20260219190905-9b9281fa8d6d // indirect
	github.com/samber/lo v1.53.0 // indirect
	github.com/samber/slog-common v0.21.0 // indirect
	github.com/samber/slog-zerolog/v2 v2.9.2 // indirect
	github.com/twmb/go-cache v1.4.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/BurntSushi/toml v1.6.0 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.5.0 // indirect
	github.com/Masterminds/sprig/v3 v3.3.0 // indirect
	github.com/bufbuild/protocompile v0.14.2-0.20260127163740-a85d2ef77713 // indirect
	github.com/conduitio/conduit-connector-protocol v0.11.0 // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/goccy/go-json v0.11.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/go-hclog v1.6.3 // indirect
	github.com/hashicorp/go-plugin v1.8.0 // indirect
	github.com/hashicorp/yamux v0.1.2 // indirect
	github.com/huandu/xstrings v1.5.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/oklog/run v1.2.0 // indirect
	github.com/petermattis/goid v0.0.0-20250319124200-ccd6737f222a // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/spf13/cast v1.7.0 // indirect
	github.com/twmb/franz-go/pkg/sr v1.8.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/exp v0.0.0-20251023183803-a4bb9ffd2546 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	golang.org/x/time v0.16.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/grpc v1.84.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Temporary fork of go-mysql v1.14.0 (ConduitIO/go-mysql). It carries two things this connector needs:
//  - canal.RowsEvent.SkippedColumns, so the CDC handler can tell a NULL from a column that a partial row
//    image (binlog_row_image=MINIMAL or NOBLOB) left out. Upstream: go-mysql-org/go-mysql#1205, PR #1206.
//  - canal.RowsEvent.TableMap, the Table_map event the rows were written under, so the handler can check
//    re-read table columns against the column types (and, with binlog_row_metadata=FULL, names) of the
//    event before it maps values onto them. ConduitIO/go-mysql#2, not proposed upstream yet.
// Pinned to the fork PR's commit until the fork is tagged v1.14.0-conduit.2 (the tag is not created here).
// Do not move to a go-mysql release that contains upstream ace1fb4 (v1.16.0 and later) without reworking the
// compressed resume of #228 first: ace1fb4 stamps the events of a compressed transaction with the payload's
// start offset instead of 0, and the resume tells them by offset 0 (see the canary test in #235).
// Remove this replace (and the ConduitIO/go-mysql fork) once a go-mysql release contains both fields:
// bump the require above to that release and delete these lines.
replace github.com/go-mysql-org/go-mysql => github.com/ConduitIO/go-mysql v1.14.1-0.20261015200357-4554b8ef277d
