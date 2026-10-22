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

package debezium

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// UnsignedOverflowError reports a BIGINT UNSIGNED value that cannot be encoded in
// the field's type without wrapping. The caller must halt with it, never emit
// the wrapped value (invariant 6).
type UnsignedOverflowError struct {
	Column string
	Value  uint64
}

func (e UnsignedOverflowError) Error() string {
	return fmt.Sprintf("BIGINT UNSIGNED value for column %s exceeds the maximum int64; "+
		"set bigint.unsigned.handling.mode to precise or string to carry it losslessly", e.Column)
}

// FormatValue converts one decoded MySQL value to the Connect-typed Go value
// its Field describes. nil (SQL NULL) is returned as nil: NULL is never
// replaced with a zero value here (invariant 6), unlike the legacy Avro path.
//
// The decoded input types are the ones the two decode paths produce (see
// schema.go in the root package): the database/sql driver ([]byte for binary,
// DECIMAL, TIME; time.Time for DATE/DATETIME/TIMESTAMP when ParseTime is on;
// int8..uint64 for integers; string for text) and go-mysql canal (string for
// text, temporal and DECIMAL; int64 for BIT and integers; []byte for binary).
// FormatValue accepts both where they can occur.
//
// A DECIMAL under SemanticDecimal (handling mode precise) is returned as the
// exact decimal text (for example "1.10"), never a float; the encoder applies
// the field's scale. A value the field cannot hold fails with an error instead
// of being coerced.
func FormatValue(f Field, v any) (any, error) {
	if v == nil {
		return nil, nil //nolint:nilnil // SQL NULL is a valid, non-error result (invariant 6)
	}

	switch f.Semantic {
	case SemanticDecimal:
		return formatDecimal(f, v)
	case SemanticDate, SemanticConnectDate:
		return formatDate(f, v)
	case SemanticMicroTime, SemanticConnectTime:
		return formatMicroTime(f, v)
	case SemanticTimestamp, SemanticMicroTimestamp, SemanticConnectTimestamp:
		return formatTimestamp(f, v)
	case SemanticZonedTimestamp:
		return formatZonedTimestamp(f, v)
	case SemanticYear:
		return formatYear(f, v)
	case SemanticBits:
		return formatBits(f, v)
	case SemanticJSON, SemanticEnum, SemanticEnumSet:
		return formatString(f, v)
	case SemanticGeometry:
		return nil, fmt.Errorf("column %s: geometry values are not decoded yet", f.Name)
	}

	switch f.Type {
	case TypeBoolean:
		return formatBool(f, v)
	case TypeInt16:
		return formatInt(f, v, math.MinInt16, math.MaxInt16)
	case TypeInt32:
		return formatInt(f, v, math.MinInt32, math.MaxInt32)
	case TypeInt64:
		return formatInt64(f, v)
	case TypeFloat32:
		return formatFloat(f, v, 32)
	case TypeFloat64:
		return formatFloat(f, v, 64)
	case TypeString:
		return formatString(f, v)
	case TypeBytes:
		return formatBytes(f, v)
	default:
		return nil, fmt.Errorf("column %s: cannot format a %s value", f.Name, f.Type)
	}
}

// formatDecimal returns the exact decimal text for a DECIMAL/NUMERIC value, or
// parses it to a float when the handling mode mapped it to a float type.
func formatDecimal(f Field, v any) (any, error) {
	s, err := decimalText(v)
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", f.Name, err)
	}
	if f.Type == TypeFloat64 {
		fl, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("column %s: a %d byte decimal is not a number: %w", f.Name, len(s), numberCause(err))
		}
		return fl, nil
	}
	return s, nil
}

func decimalText(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	default:
		return "", fmt.Errorf("expected a decimal string or bytes, got %T", v)
	}
}

// formatDate returns the number of days since the Unix epoch.
func formatDate(f Field, v any) (any, error) {
	t, err := asUTCTime(v, []string{time.DateOnly})
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", f.Name, err)
	}
	return int32(t.Unix() / 86400), nil //nolint:gosec // MySQL DATE spans ~2.9M days, always within int32
}

// formatMicroTime returns the number of microseconds since midnight.
func formatMicroTime(f Field, v any) (any, error) {
	switch x := v.(type) {
	case time.Time:
		return timeOfDayMicros(x), nil
	case string:
		return parseTimeString(x)
	case []byte:
		return parseTimeString(string(x))
	default:
		return nil, fmt.Errorf("column %s: expected a time value, got %T", f.Name, v)
	}
}

// formatTimestamp returns a DATETIME as milliseconds (precision 0-3) or
// microseconds (precision 4-6) past the epoch, read as UTC with no shift.
func formatTimestamp(f Field, v any) (any, error) {
	t, err := asUTCTime(v, []string{"2006-01-02 15:04:05.999999", "2006-01-02 15:04:05", time.RFC3339Nano})
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", f.Name, err)
	}
	micros := t.Unix()*1_000_000 + int64(t.Nanosecond()/1000)
	if f.Semantic == SemanticConnectTimestamp || f.Semantic == SemanticTimestamp {
		return micros / 1000, nil // milliseconds
	}
	return micros, nil
}

// formatZonedTimestamp returns a TIMESTAMP as an ISO 8601 UTC string with
// microsecond precision, e.g. "2018-06-20T13:37:03.000000Z".
func formatZonedTimestamp(f Field, v any) (any, error) {
	t, err := asUTCTime(v, []string{"2006-01-02 15:04:05.999999", "2006-01-02 15:04:05", time.RFC3339Nano})
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", f.Name, err)
	}
	return t.UTC().Format("2006-01-02T15:04:05.000000Z"), nil
}

// formatYear returns a YEAR column as the integer year.
func formatYear(f Field, v any) (any, error) {
	switch x := v.(type) {
	case int64:
		return int32(x), nil //nolint:gosec // MySQL YEAR is 1901-2155 (or 0000), always within int32
	case int32:
		return x, nil
	case int:
		return int32(x), nil //nolint:gosec // MySQL YEAR is 1901-2155 (or 0000), always within int32
	case []byte:
		return formatYear(f, string(x))
	case string:
		y, err := strconv.ParseInt(x, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("column %s: a %d byte year is not a number: %w", f.Name, len(x), numberCause(err))
		}
		return int32(y), nil
	default:
		return nil, fmt.Errorf("column %s: expected a year value, got %T", f.Name, v)
	}
}

// formatBits returns a BIT(>1) column as little-endian bytes sized to the
// column's bit length. The driver returns big-endian bytes, canal an int64.
func formatBits(f Field, v any) (any, error) {
	bits := f.paramInt("length")
	if bits <= 0 {
		bits = 8
	}
	n := (bits + 7) / 8

	switch x := v.(type) {
	case []byte:
		out := make([]byte, n)
		// Right-align, then reverse to little-endian (the driver is big-endian).
		copy(out[n-min(len(x), n):], x[max(0, len(x)-n):])
		reverseBytes(out)
		return out, nil
	case int64:
		out := make([]byte, 8)
		binary.LittleEndian.PutUint64(out, uint64(x)) //nolint:gosec // BIT is a bit pattern; reinterpretation is the point
		return out[:n], nil
	case string:
		return formatBits(f, []byte(x))
	default:
		return nil, fmt.Errorf("column %s: expected a bit value, got %T", f.Name, v)
	}
}

func formatBool(f Field, v any) (any, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case int64:
		return x != 0, nil
	case int8, int16, int32, int:
		n, err := toInt64(x)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", f.Name, err)
		}
		return n != 0, nil
	case uint8, uint16, uint32, uint64:
		u, _ := toUint64(x)
		return u != 0, nil
	case []byte:
		return len(x) > 0 && x[0] != 0, nil
	default:
		return nil, fmt.Errorf("column %s: expected a boolean value, got %T", f.Name, v)
	}
}

// formatInt returns an integer narrowed to the field's width, failing if the
// value does not fit rather than truncating.
func formatInt(f Field, v any, lo, hi int64) (any, error) {
	n, err := toInt64(v)
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", f.Name, err)
	}
	if n < lo || n > hi {
		return nil, fmt.Errorf("column %s: value does not fit %s", f.Name, f.Type)
	}
	switch f.Type {
	case TypeInt16:
		return int16(n), nil //nolint:gosec // range-checked against lo/hi above
	case TypeInt32:
		return int32(n), nil //nolint:gosec // range-checked against lo/hi above
	default:
		return n, nil
	}
}

// formatInt64 returns an integer as int64. A BIGINT UNSIGNED above 2^63-1 in
// long mode fails with UnsignedOverflowError rather than wrapping.
func formatInt64(f Field, v any) (any, error) {
	if u, ok := toUint64(v); ok && u > math.MaxInt64 {
		return nil, UnsignedOverflowError{Column: f.Name, Value: u}
	}
	n, err := toInt64(v)
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", f.Name, err)
	}
	return n, nil
}

func formatFloat(f Field, v any, bits int) (any, error) {
	switch x := v.(type) {
	case float64:
		if bits == 32 {
			return float32(x), nil
		}
		return x, nil
	case float32:
		if bits == 32 {
			return x, nil
		}
		return float64(x), nil
	case string:
		fl, err := strconv.ParseFloat(x, bits)
		if err != nil {
			return nil, fmt.Errorf("column %s: a %d byte value is not a number: %w", f.Name, len(x), numberCause(err))
		}
		return fl, nil
	case []byte:
		return formatFloat(f, string(x), bits)
	default:
		n, err := toInt64(v)
		if err == nil {
			if bits == 32 {
				return float32(n), nil
			}
			return float64(n), nil
		}
		return nil, fmt.Errorf("column %s: expected a float value, got %T", f.Name, v)
	}
}

func formatString(f Field, v any) (any, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	default:
		return nil, fmt.Errorf("column %s: expected a string value, got %T", f.Name, v)
	}
}

func formatBytes(f Field, v any) (any, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	default:
		return nil, fmt.Errorf("column %s: expected bytes, got %T", f.Name, v)
	}
}

func (f Field) paramInt(key string) int {
	if f.Parameters == nil {
		return 0
	}
	n, _ := strconv.Atoi(f.Parameters[key])
	return n
}

// asUTCTime coerces a time.Time or a textual temporal value to a time.Time in
// UTC, trying the given layouts in order.
func asUTCTime(v any, layouts []string) (time.Time, error) {
	switch x := v.(type) {
	case time.Time:
		return x.UTC(), nil
	case string:
		return parseUTCTime(x, layouts)
	case []byte:
		return parseUTCTime(string(x), layouts)
	default:
		return time.Time{}, fmt.Errorf("expected a temporal value, got %T", v)
	}
}

func parseUTCTime(s string, layouts []string) (time.Time, error) {
	s = strings.TrimSpace(s)
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse a %d byte temporal value", len(s))
}

func timeOfDayMicros(t time.Time) int64 {
	return int64(t.Hour()*3600+t.Minute()*60+t.Second())*1_000_000 + int64(t.Nanosecond()/1000)
}

// parseTimeString parses "HH:MM:SS[.ffffff]" into microseconds since midnight.
func parseTimeString(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty time value")
	}
	var frac int64
	if i := strings.IndexByte(s, '.'); i >= 0 {
		digits := s[i+1:]
		s = s[:i]
		if len(digits) > 6 {
			digits = digits[:6]
		}
		for len(digits) < 6 {
			digits += "0"
		}
		f, err := strconv.ParseInt(digits, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid fractional time")
		}
		frac = f
	}
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid time value (%d bytes)", len(s))
	}
	h, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	sec, err3 := strconv.Atoi(parts[2])
	if err1 != nil || err2 != nil || err3 != nil {
		return 0, fmt.Errorf("invalid time value (%d bytes)", len(s))
	}
	return int64(h*3600+m*60+sec)*1_000_000 + frac, nil
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		if x > math.MaxInt64 {
			return 0, fmt.Errorf("unsigned value exceeds int64")
		}
		return int64(x), nil
	default:
		return 0, fmt.Errorf("expected an integer value, got %T", v)
	}
}

func toUint64(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint:
		return uint64(x), true
	case uint8:
		return uint64(x), true
	case uint16:
		return uint64(x), true
	case uint32:
		return uint64(x), true
	case uint64:
		return x, true
	case int:
		if x >= 0 {
			return uint64(x), true
		}
	case int64:
		if x >= 0 {
			return uint64(x), true
		}
	}
	return 0, false
}

func reverseBytes(b []byte) {
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
}

// numberCause returns the reason a number failed to parse without the number,
// which strconv.NumError repeats. Errors reach the DLQ, the API and CLI
// output, so a record value must not be in them.
func numberCause(err error) error {
	var ne *strconv.NumError
	if errors.As(err, &ne) {
		return ne.Err
	}
	return fmt.Errorf("not a valid number")
}
