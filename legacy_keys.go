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
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

// Snapshot positions written before keys were tagged (envelope version 0 or 1)
// store a bare JSON string for a key whose Go type was []byte or time.Time, and
// the position does not record the column type. Reading such a string as the
// key itself seeks from the wrong value: for a VARCHAR key "ZA==" (the base64 of
// "d") sorts after every real value, so the resume skipped the rest of the table.
//
// Invariant 3: resolveLegacyKeys turns each legacy key into the value the old
// build meant, using the key column's actual type, or, when it cannot, forgets
// the table's position so its snapshot restarts from the first row. That costs
// duplicates, never a gap.

// legacyBase64Types are the information_schema DATA_TYPEs whose driver value is
// []byte, which the old encoder wrote as base64.
var legacyBase64Types = map[string]bool{
	"char": true, "varchar": true, "tinytext": true, "text": true, "mediumtext": true, "longtext": true,
	"binary": true, "varbinary": true, "tinyblob": true, "blob": true, "mediumblob": true, "longblob": true,
	"decimal": true, "numeric": true, "time": true, "enum": true, "set": true, "bit": true,
}

// legacyTimeTypes are DATA_TYPEs whose driver value was a time.Time, written as
// RFC 3339 text, which MySQL accepts back as a query argument.
var legacyTimeTypes = map[string]bool{"date": true, "datetime": true, "timestamp": true}

// resolveLegacyKeys rewrites every legacy key in pos in place. keyColumns gives
// the configured key columns per table, used when a legacy composite item
// carries no key name (old builds never filled it in).
func resolveLegacyKeys(
	ctx context.Context, db *sqlx.DB, database string, pos *common.SnapshotPosition, keyColumns common.TableKeys,
) error {
	for table, tp := range pos.Snapshots {
		if _, configured := keyColumns[table]; !configured {
			// No longer read: nothing resumes from it. Leave the entry as it was.
			continue
		}

		types, err := columnTypes(db, database, table)
		if err != nil {
			return err
		}

		resolved, reason := resolveTablePosition(tp, keyColumns[table], types)
		if reason != "" {
			sdk.Logger(ctx).Warn().Str("table", table).
				Msg("cannot use the saved snapshot position for this table (" + reason +
					"); restarting its snapshot from the first row. Rows already delivered will be delivered again")
			delete(pos.Snapshots, table)
			continue
		}
		pos.Snapshots[table] = resolved
	}

	return nil
}

func columnTypes(db *sqlx.DB, database, table string) (map[string]string, error) {
	var cols []struct {
		Name     string `db:"COLUMN_NAME"`
		DataType string `db:"DATA_TYPE"`
	}
	err := db.Select(&cols, `
		SELECT COLUMN_NAME, DATA_TYPE
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
	`, database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to read column types of table %q: %w", table, err)
	}

	types := make(map[string]string, len(cols))
	for _, c := range cols {
		types[strings.ToLower(c.Name)] = strings.ToLower(c.DataType)
	}
	return types, nil
}

// resolveTablePosition converts one table's legacy keys. A non-empty reason
// means the position cannot be trusted and the table must restart.
func resolveTablePosition(
	tp common.TablePosition, keys common.PrimaryKeys, types map[string]string,
) (common.TablePosition, string) {
	switch {
	case tp.SingleKey != nil:
		if len(keys) != 1 {
			return tp, "the table's key columns changed"
		}
		lastRead, reason := resolveLegacyValue(tp.SingleKey.LastRead, keys[0], types)
		if reason == "" {
			reason = checkKeyClass(lastRead, keys[0], types)
		}
		if reason != "" {
			return tp, reason
		}
		// SnapshotEnd is informational (the end is recomputed on resume).
		snapshotEnd, endReason := resolveLegacyValue(tp.SingleKey.SnapshotEnd, keys[0], types)
		if endReason == "" {
			endReason = checkKeyClass(snapshotEnd, keys[0], types)
		}
		if endReason != "" {
			snapshotEnd = nil
		}
		return common.TablePosition{SingleKey: &common.TablePositionSingleKey{LastRead: lastRead, SnapshotEnd: snapshotEnd}}, ""

	case tp.MultipleKey != nil:
		items := *tp.MultipleKey
		if len(items) != len(keys) {
			return tp, "the table's key columns changed"
		}
		out := make(common.TablePositionMultipleKey, len(items))
		for i, item := range items {
			column := item.KeyName
			if column == "" {
				column = keys[i]
			}
			lastRead, reason := resolveLegacyValue(item.LastRead, column, types)
			if reason == "" {
				reason = checkKeyClass(lastRead, column, types)
			}
			if reason != "" {
				return tp, reason
			}
			snapshotEnd, endReason := resolveLegacyValue(item.SnapshotEnd, column, types)
			if endReason == "" {
				endReason = checkKeyClass(snapshotEnd, column, types)
			}
			if endReason != "" {
				snapshotEnd = nil
			}
			out[i] = common.TablePositionMultipleKeyItem{KeyName: column, LastRead: lastRead, SnapshotEnd: snapshotEnd}
		}
		return common.TablePosition{MultipleKey: &out}, ""
	}

	return tp, ""
}

// resolveLegacyValue returns v unchanged unless it is a LegacyKey.
func resolveLegacyValue(v any, column string, types map[string]string) (any, string) {
	legacy, ok := v.(common.LegacyKey)
	if !ok {
		return v, ""
	}

	dataType, known := types[strings.ToLower(column)]
	switch {
	case !known:
		return nil, fmt.Sprintf("key column %q not found", column)
	case legacyBase64Types[dataType]:
		raw, err := base64.StdEncoding.DecodeString(string(legacy))
		if err != nil {
			return nil, fmt.Sprintf("key of column %q (%s) is not valid base64", column, dataType)
		}
		return raw, ""
	case legacyTimeTypes[dataType]:
		return string(legacy), ""
	default:
		return nil, fmt.Sprintf("unexpected text key for column %q of type %s", column, dataType)
	}
}

var (
	numericKeyTypes = map[string]bool{
		"tinyint": true, "smallint": true, "mediumint": true, "int": true, "integer": true, "bigint": true,
		"float": true, "double": true, "real": true, "year": true,
	}
	// byteKeyTypes are the types whose driver value is []byte.
	byteKeyTypes = legacyBase64Types
)

// checkKeyClass reports why v cannot be a key of the column's current type, or
// "" when it can. The saved position says what the key was when it was written;
// if the column has since been retyped (INT to VARCHAR, say), seeking from that
// value compares across classes and MySQL's implicit conversion can skip rows
// (last_read 3 on a VARCHAR column holding a..f resumed with none of them).
// Invariant 3: restart the table instead; duplicates, never loss.
//
// The classes are numeric, byte/text (including DECIMAL, TIME, ENUM) and
// temporal. A column type outside the known lists is not checked.
func checkKeyClass(v any, column string, types map[string]string) string {
	if v == nil {
		return ""
	}
	dataType, known := types[strings.ToLower(column)]
	if !known {
		return fmt.Sprintf("key column %q not found", column)
	}

	var ok bool
	switch {
	case numericKeyTypes[dataType]:
		switch x := v.(type) {
		case int64, uint64, float32, float64:
			ok = true
		case []byte:
			// The driver returns a BIGINT UNSIGNED above the int64 range as its
			// decimal digits. Only a numeric string is a numeric key.
			_, err := strconv.ParseFloat(string(x), 64)
			ok = err == nil
		}
	case byteKeyTypes[dataType]:
		switch v.(type) {
		case []byte, string:
			ok = true
		}
	case legacyTimeTypes[dataType]:
		switch v.(type) {
		case time.Time, string:
			ok = true
		}
	default:
		return ""
	}

	if !ok {
		return fmt.Sprintf("the saved key is a %T but column %q is now %s: the column was retyped", v, column, dataType)
	}
	return ""
}
