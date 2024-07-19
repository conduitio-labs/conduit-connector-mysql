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
	"errors"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type combinedIterator struct {
	snapshotIterator common.Iterator
	cdcIterator      common.Iterator

	currentIterator common.Iterator
}

type combinedIteratorConfig struct {
	snapshotConfig snapshotIteratorConfig
	cdcConfig      cdcIteratorConfig
}

func newCombinedIterator(
	ctx context.Context,
	config combinedIteratorConfig,
) (common.Iterator, error) {
	iterator := &combinedIterator{}
	var err error

	iterator.cdcIterator, err = newCdcIterator(ctx, config.cdcConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create cdc iterator: %w", err)
	}

	iterator.snapshotIterator, err = newSnapshotIterator(ctx, config.snapshotConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot iterator: %w", err)
	}

	iterator.currentIterator = iterator.snapshotIterator

	return iterator, nil
}

func (c *combinedIterator) Ack(ctx context.Context, pos sdk.Position) error {
	//nolint:wrapcheck // error already wrapped in iterator
	return c.currentIterator.Ack(ctx, pos)
}

func (c *combinedIterator) Next(ctx context.Context) (sdk.Record, error) {
	rec, err := c.currentIterator.Next(ctx)
	if errors.Is(err, ErrSnapshotIteratorDone) {
		c.currentIterator = c.cdcIterator
		//nolint:wrapcheck // error already wrapped in iterator
		return c.currentIterator.Next(ctx)
	} else if err != nil {
		return sdk.Record{}, fmt.Errorf("failed to get next record: %w", err)
	}

	return rec, nil
}

func (c *combinedIterator) Teardown(ctx context.Context) error {
	var errs []error

	if c.snapshotIterator != nil {
		err := c.snapshotIterator.Teardown(ctx)
		errs = append(errs, err)
	}

	if c.cdcIterator != nil {
		err := c.cdcIterator.Teardown(ctx)
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}
