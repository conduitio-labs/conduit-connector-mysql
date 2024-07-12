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
	"fmt"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

type Source struct {
	sdk.UnimplementedSource

	config common.SourceConfig

	db *sqlx.DB

	iterator common.Iterator
}

func NewSource() sdk.Source {
	// Create Source and wrap it in the default middleware.
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	// Parameters is a map of named Parameters that describe how to configure
	// the Source. Parameters can be generated from SourceConfig with paramgen.
	return s.config.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	if err := sdk.Util.ParseConfig(cfg, &s.config); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("configured source connector")
	return nil
}

func (s *Source) Open(ctx context.Context, _ sdk.Position) (err error) {
	s.db, err = common.NewSqlxDB(s.config.Config)
	if err != nil {
		return fmt.Errorf("failed to connect to mysql: %w", err)
	}

	tableKeys, err := getTableKeys(s.db, s.config.Database, s.config.Tables)
	if err != nil {
		return fmt.Errorf("failed to get table keys: %w", err)
	}

	s.iterator, err = newCombinedIterator(ctx, combinedIteratorConfig{
		snapshotConfig: snapshotIteratorConfig{
			db:        s.db,
			tableKeys: tableKeys,
			database:  s.config.Database,
			tables:    s.config.Tables,
		},
		cdcConfig: cdcIteratorConfig{
			SourceConfig: common.SourceConfig{
				Config: s.config.Config,
				Tables: s.config.Tables,
			},
			TableKeys: tableKeys,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create snapshot iterator: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("opened source connector")

	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	//nolint:wrapcheck // error already wrapped in iterator
	return s.iterator.Next(ctx)
}

func (s *Source) Ack(ctx context.Context, _ sdk.Position) error {
	//nolint:wrapcheck // error already wrapped in iterator
	return s.iterator.Ack(ctx, sdk.Position{})
}

func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		//nolint:wrapcheck // error already wrapped in iterator
		return s.iterator.Teardown(ctx)
	}

	return nil
}
