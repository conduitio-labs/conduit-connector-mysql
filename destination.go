// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

type Destination struct {
	sdk.UnimplementedDestination

	db     *sqlx.DB
	config common.DestinationConfig
}

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Parameters() config.Parameters {
	return d.config.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg config.Config) error {
	sdk.Logger(ctx).Info().Msg("Configuring Destination...")
	err := sdk.Util.ParseConfig(ctx, cfg, &d.config, d.config.Parameters())
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

func (d *Destination) Open(_ context.Context) (err error) {
	d.db, err = sqlx.Open("mysql", d.config.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to mysql: %w", err)
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, recs []opencdc.Record) (int, error) {
	for i, rec := range recs {
		switch rec.Operation {
		case opencdc.OperationSnapshot:
			if err := d.upsertRecord(ctx, rec); err != nil {
				return i, err
			}
		case opencdc.OperationCreate:
			if err := d.upsertRecord(ctx, rec); err != nil {
				return i, err
			}
		case opencdc.OperationUpdate:
			if err := d.upsertRecord(ctx, rec); err != nil {
				return i, err
			}
		case opencdc.OperationDelete:
			if err := d.deleteRecord(ctx, rec); err != nil {
				return i, err
			}
		}
	}

	return len(recs), nil
}

func (d *Destination) Teardown(_ context.Context) error {
	if d.db != nil {
		if err := d.db.Close(); err != nil {
			return fmt.Errorf("failed to close connection: %w", err)
		}
	}

	return nil
}

func (d *Destination) upsertRecord(ctx context.Context, rec opencdc.Record) error {
	payload, isStructured := rec.Payload.After.(opencdc.StructuredData)
	if !isStructured {
		data := make(opencdc.StructuredData)
		if err := json.Unmarshal(rec.Payload.After.Bytes(), &data); err != nil {
			return fmt.Errorf("failed to json unmarshal non structured data: %w", err)
		}

		payload = data
	}

	columns := make([]string, 0, len(payload))
	placeholders := make([]string, 0, len(payload))
	values := make([]any, 0, len(payload))
	upsertList := make([]string, 0, len(payload))

	for col, val := range payload {
		columns = append(columns, col)
		placeholders = append(placeholders, "?")
		values = append(values, val)

		upsertList = append(upsertList, fmt.Sprint(col, " = VALUES(", col, ")"))
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (%s)
		VALUES (%s)
		ON DUPLICATE KEY UPDATE %s;`,
		d.config.Table, strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(upsertList, ", "),
	)
	_, err := d.db.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("failed to insert record: %w", err)
	}

	return nil
}

func (d *Destination) deleteRecord(ctx context.Context, rec opencdc.Record) error {
	val, err := d.parseRecordKey(rec.Key)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE %s = ?;`,
		d.config.Table, d.config.Key,
	)
	_, err = d.db.ExecContext(ctx, query, val)
	if err != nil {
		return fmt.Errorf("failed to insert record: %w", err)
	}

	return nil
}

func (d *Destination) parseRecordKey(key opencdc.Data) (any, error) {
	data := make(opencdc.StructuredData)
	if err := json.Unmarshal(key.Bytes(), &data); err != nil {
		return nil, fmt.Errorf("failed to parse key: %w", err)
	}

	val, ok := data[d.config.Key]
	if !ok {
		return nil, fmt.Errorf("primary key not found")
	}

	return val, nil
}
