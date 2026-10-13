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

package common

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
)

// Snapshot positions record the last key read per table. The values come
// straight from the MySQL driver and the fetch workers feed them back to the
// database as query arguments on resume, so they must come back as the same
// Go type and value. Plain JSON does not guarantee that:
//
//   - integers above 2^53 round to float64;
//   - uint64 values above 2^63-1 round or overflow;
//   - []byte (DECIMAL, CHAR/VARCHAR, BINARY/VARBINARY, TIME) becomes base64 text
//     and comes back as a string that no longer compares like the original;
//   - time.Time becomes RFC 3339 text.
//
// Each value is therefore written as a tagged object, e.g.
// {"t":"int64","v":"9007199254740993"}, and decoded back to its original Go
// type. A value written by an older build is a bare JSON scalar; it is still
// read (see decodeKeyValue) so existing positions keep working.

// LegacyKey is a bare JSON string read from a snapshot position written before
// keys were tagged (envelope version 0 or 1). What it means depends on the type
// of the key column, which a position does not record: the old encoder wrote a
// []byte value (CHAR/VARCHAR/TEXT, BINARY/VARBINARY/BLOB, DECIMAL, TIME) as
// base64 text, and a time.Time value (DATE, DATETIME, TIMESTAMP) as RFC 3339
// text. It must be resolved against the column type before it is used as a
// query argument (see resolveLegacyKeys in package mysql); using it as is would
// seek from the wrong key and silently skip rows. Never returned for positions
// written by this build.
type LegacyKey string

const (
	keyTypeInt64   = "int64"
	keyTypeUint64  = "uint64"
	keyTypeFloat32 = "float32"
	keyTypeFloat64 = "float64"
	keyTypeBytes   = "bytes"
	keyTypeString  = "string"
	keyTypeTime    = "time"
	keyTypeBool    = "bool"
)

type taggedKeyValue struct {
	Type  string `json:"t"`
	Value string `json:"v"`
}

// encodeKeyValue renders a key value as JSON, type-exactly. nil is null.
func encodeKeyValue(v any) (json.RawMessage, error) {
	var t taggedKeyValue
	switch x := v.(type) {
	case nil:
		return json.RawMessage("null"), nil
	case int:
		t = taggedKeyValue{keyTypeInt64, strconv.FormatInt(int64(x), 10)}
	case int8:
		t = taggedKeyValue{keyTypeInt64, strconv.FormatInt(int64(x), 10)}
	case int16:
		t = taggedKeyValue{keyTypeInt64, strconv.FormatInt(int64(x), 10)}
	case int32:
		t = taggedKeyValue{keyTypeInt64, strconv.FormatInt(int64(x), 10)}
	case int64:
		t = taggedKeyValue{keyTypeInt64, strconv.FormatInt(x, 10)}
	case uint:
		t = taggedKeyValue{keyTypeUint64, strconv.FormatUint(uint64(x), 10)}
	case uint8:
		t = taggedKeyValue{keyTypeUint64, strconv.FormatUint(uint64(x), 10)}
	case uint16:
		t = taggedKeyValue{keyTypeUint64, strconv.FormatUint(uint64(x), 10)}
	case uint32:
		t = taggedKeyValue{keyTypeUint64, strconv.FormatUint(uint64(x), 10)}
	case uint64:
		t = taggedKeyValue{keyTypeUint64, strconv.FormatUint(x, 10)}
	case float32:
		t = taggedKeyValue{keyTypeFloat32, strconv.FormatFloat(float64(x), 'g', -1, 32)}
	case float64:
		t = taggedKeyValue{keyTypeFloat64, strconv.FormatFloat(x, 'g', -1, 64)}
	case []byte:
		t = taggedKeyValue{keyTypeBytes, base64.StdEncoding.EncodeToString(x)}
	case string:
		t = taggedKeyValue{keyTypeString, x}
	case time.Time:
		t = taggedKeyValue{keyTypeTime, x.Format(time.RFC3339Nano)}
	case bool:
		t = taggedKeyValue{keyTypeBool, strconv.FormatBool(x)}
	case LegacyKey:
		// An unresolved legacy key (a table no longer read) round-trips as it was.
		raw, err := json.Marshal(string(x))
		if err != nil {
			return nil, fmt.Errorf("failed to encode snapshot key: %w", err)
		}
		return raw, nil
	default:
		return nil, fmt.Errorf("unsupported snapshot key type %T", v)
	}

	raw, err := json.Marshal(t)
	if err != nil {
		return nil, fmt.Errorf("failed to encode snapshot key: %w", err)
	}
	return raw, nil
}

// decodeKeyValue is the inverse of encodeKeyValue. A bare JSON scalar, as written
// by builds before the tagged form existed, is accepted: numbers become int64,
// uint64 or float64 (exactly, instead of always float64); strings, booleans
// and null are returned as they were.
//
// A legacy bare string is returned as a LegacyKey, not a string: it is base64
// text for text, binary and DECIMAL columns, and the position does not say which
// column type it came from. The snapshot resolves it against the key column's
// type, and restarts the table from its first row (duplicates, never a gap) when
// it cannot.
func decodeKeyValue(raw json.RawMessage) (any, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil //nolint:nilnil // null is a valid key value (no position yet)
	}

	switch raw[0] {
	case '{':
		var t taggedKeyValue
		if err := json.Unmarshal(raw, &t); err != nil {
			return nil, fmt.Errorf("invalid snapshot key: %w", err)
		}
		return decodeTagged(t)
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("invalid snapshot key: %w", err)
		}
		return LegacyKey(s), nil
	case 't', 'f':
		var b bool
		if err := json.Unmarshal(raw, &b); err != nil {
			return nil, fmt.Errorf("invalid snapshot key: %w", err)
		}
		return b, nil
	default:
		return decodeLegacyNumber(string(raw))
	}
}

func decodeLegacyNumber(s string) (any, error) {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, nil
	}
	if u, err := strconv.ParseUint(s, 10, 64); err == nil {
		return u, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid snapshot key number %q: %w", s, err)
	}
	return f, nil
}

func decodeTagged(t taggedKeyValue) (any, error) {
	wrap := func(err error) (any, error) {
		return nil, fmt.Errorf("invalid snapshot key of type %q: %w", t.Type, err)
	}
	switch t.Type {
	case keyTypeInt64:
		v, err := strconv.ParseInt(t.Value, 10, 64)
		if err != nil {
			return wrap(err)
		}
		return v, nil
	case keyTypeUint64:
		v, err := strconv.ParseUint(t.Value, 10, 64)
		if err != nil {
			return wrap(err)
		}
		return v, nil
	case keyTypeFloat32:
		v, err := strconv.ParseFloat(t.Value, 32)
		if err != nil {
			return wrap(err)
		}
		return float32(v), nil
	case keyTypeFloat64:
		v, err := strconv.ParseFloat(t.Value, 64)
		if err != nil || math.IsNaN(v) {
			return wrap(fmt.Errorf("not a usable float: %q", t.Value))
		}
		return v, nil
	case keyTypeBytes:
		v, err := base64.StdEncoding.DecodeString(t.Value)
		if err != nil {
			return wrap(err)
		}
		return v, nil
	case keyTypeString:
		return t.Value, nil
	case keyTypeTime:
		v, err := time.Parse(time.RFC3339Nano, t.Value)
		if err != nil {
			return wrap(err)
		}
		return v, nil
	case keyTypeBool:
		v, err := strconv.ParseBool(t.Value)
		if err != nil {
			return wrap(err)
		}
		return v, nil
	default:
		return nil, fmt.Errorf("unknown snapshot key encoding %q; this position was written by a newer connector build", t.Type)
	}
}

// keyPair is the wire form shared by single and per-column composite keys.
type keyPair struct {
	LastRead    json.RawMessage `json:"last_read"`
	SnapshotEnd json.RawMessage `json:"snapshot_end"`
	KeyType     string          `json:"key_type,omitempty"`
}

func encodeKeyPair(lastRead, snapshotEnd any, keyType string) (keyPair, error) {
	lr, err := encodeKeyValue(lastRead)
	if err != nil {
		return keyPair{}, err
	}
	se, err := encodeKeyValue(snapshotEnd)
	if err != nil {
		return keyPair{}, err
	}
	return keyPair{LastRead: lr, SnapshotEnd: se, KeyType: keyType}, nil
}

func decodeKeyPair(p keyPair) (lastRead, snapshotEnd any, err error) {
	if lastRead, err = decodeKeyValue(p.LastRead); err != nil {
		return nil, nil, err
	}
	if snapshotEnd, err = decodeKeyValue(p.SnapshotEnd); err != nil {
		return nil, nil, err
	}
	return lastRead, snapshotEnd, nil
}

// MarshalJSON writes the keys type-exactly.
func (p TablePositionSingleKey) MarshalJSON() ([]byte, error) {
	pair, err := encodeKeyPair(p.LastRead, p.SnapshotEnd, p.KeyType)
	if err != nil {
		return nil, err
	}
	return json.Marshal(pair) //nolint:wrapcheck // plain struct
}

// UnmarshalJSON reads both the type-exact and the legacy form.
func (p *TablePositionSingleKey) UnmarshalJSON(data []byte) error {
	var pair keyPair
	if err := json.Unmarshal(data, &pair); err != nil {
		return fmt.Errorf("invalid single key position: %w", err)
	}
	lr, se, err := decodeKeyPair(pair)
	if err != nil {
		return err
	}
	p.LastRead, p.SnapshotEnd, p.KeyType = lr, se, pair.KeyType
	return nil
}

// MarshalJSON writes the keys type-exactly.
func (p TablePositionMultipleKeyItem) MarshalJSON() ([]byte, error) {
	pair, err := encodeKeyPair(p.LastRead, p.SnapshotEnd, p.KeyType)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct { //nolint:wrapcheck // plain struct
		KeyName string `json:"key_name"`
		keyPair
	}{p.KeyName, pair})
}

// UnmarshalJSON reads both the type-exact and the legacy form.
func (p *TablePositionMultipleKeyItem) UnmarshalJSON(data []byte) error {
	var aux struct {
		KeyName string `json:"key_name"`
		keyPair
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return fmt.Errorf("invalid multiple key position: %w", err)
	}
	lr, se, err := decodeKeyPair(aux.keyPair)
	if err != nil {
		return err
	}
	p.KeyName, p.LastRead, p.SnapshotEnd, p.KeyType = aux.KeyName, lr, se, aux.KeyType
	return nil
}
