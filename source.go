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

//go:generate paramgen -output=paramgen_src.go SourceConfig

import (
	"context"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"

	// apply mysql driver.
	_ "github.com/go-sql-driver/mysql"
)

type Source struct {
	sdk.UnimplementedSource

	config SourceConfig

	db *sqlx.DB
}

type SourceConfig struct {
	Config

	Tables []string `json:"tables" validate:"required"`
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
	s.db, err = connect(s.config.Config)
	if err != nil {
		return fmt.Errorf("failed to connect to mysql: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("opened source connector")
	return nil
}

func (s *Source) Read(_ context.Context) (sdk.Record, error) {
	return sdk.Record{}, nil
}

func (s *Source) Ack(_ context.Context, _ sdk.Position) error {
	// Ack signals to the implementation that the record with the supplied
	// position was successfully processed. This method might be called after
	// the context of Read is already cancelled, since there might be
	// outstanding acks that need to be delivered. When Teardown is called it is
	// guaranteed there won't be any more calls to Ack.
	// Ack can be called concurrently with Read.
	return nil
}

func (s *Source) Teardown(_ context.Context) error {
	// Teardown signals to the plugin that there will be no more calls to any
	// other function. After Teardown returns, the plugin should be ready for a
	// graceful shutdown.
	return nil
}
