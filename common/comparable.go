// First, replace the existing position logic with the new types:

package common

import (
	"database/sql"
	"fmt"
	"time"
)

type Comparable interface {
	Less(other Comparable) bool
	Equal(other Comparable) bool
	String() string
}

// Basic comparable types
type (
	IntComparable    struct{ Value int64 }
	UintComparable   struct{ Value uint64 }
	StringComparable struct{ Value string }
	TimeComparable   struct{ Value time.Time }
	FloatComparable  struct{ Value float64 }
)

func (c *IntComparable) Less(other Comparable) bool {
	if o, ok := other.(*IntComparable); ok {
		return c.Value < o.Value
	}
	return false
}

func (c *IntComparable) Equal(other Comparable) bool {
	if o, ok := other.(*IntComparable); ok {
		return c.Value == o.Value
	}
	return false
}
func (c *IntComparable) String() string { return fmt.Sprintf("%d", c.Value) }

func (c *UintComparable) Less(other Comparable) bool {
	if o, ok := other.(*UintComparable); ok {
		return c.Value < o.Value
	}
	return false
}

func (c *UintComparable) Equal(other Comparable) bool {
	if o, ok := other.(*UintComparable); ok {
		return c.Value == o.Value
	}
	return false
}
func (c *UintComparable) String() string { return fmt.Sprintf("%d", c.Value) }

func (c *StringComparable) Less(other Comparable) bool {
	if o, ok := other.(*StringComparable); ok {
		return c.Value < o.Value
	}
	return false
}

func (c *StringComparable) Equal(other Comparable) bool {
	if o, ok := other.(*StringComparable); ok {
		return c.Value == o.Value
	}
	return false
}
func (c *StringComparable) String() string { return c.Value }

func (c *TimeComparable) Less(other Comparable) bool {
	if o, ok := other.(*TimeComparable); ok {
		return c.Value.Before(o.Value)
	}
	return false
}

func (c *TimeComparable) Equal(other Comparable) bool {
	if o, ok := other.(*TimeComparable); ok {
		return c.Value.Equal(o.Value)
	}
	return false
}
func (c *TimeComparable) String() string { return c.Value.Format(time.RFC3339Nano) }

func (c *FloatComparable) Less(other Comparable) bool {
	if o, ok := other.(*FloatComparable); ok {
		return c.Value < o.Value
	}
	return false
}

func (c *FloatComparable) Equal(other Comparable) bool {
	if o, ok := other.(*FloatComparable); ok {
		return c.Value == o.Value
	}
	return false
}
func (c *FloatComparable) String() string { return fmt.Sprintf("%g", c.Value) }

func NewComparable(v any) (Comparable, error) {
	switch val := v.(type) {
	case nil:
		return nil, fmt.Errorf("cannot create comparable from nil value")
	case int64:
		return &IntComparable{val}, nil
	case int32:
		return &IntComparable{int64(val)}, nil
	case int:
		return &IntComparable{int64(val)}, nil
	case uint64:
		return &UintComparable{val}, nil
	case uint32:
		return &UintComparable{uint64(val)}, nil
	case uint:
		return &UintComparable{uint64(val)}, nil
	case float64:
		return &FloatComparable{val}, nil
	case float32:
		return &FloatComparable{float64(val)}, nil
	case string:
		return &StringComparable{val}, nil
	case []byte:
		return &StringComparable{string(val)}, nil
	case time.Time:
		return &TimeComparable{val}, nil
	case sql.NullString:
		if !val.Valid {
			return nil, fmt.Errorf("cannot compare null value")
		}
		return &StringComparable{val.String}, nil
	case sql.NullInt64:
		if !val.Valid {
			return nil, fmt.Errorf("cannot compare null value")
		}
		return &IntComparable{val.Int64}, nil
	case sql.NullFloat64:
		if !val.Valid {
			return nil, fmt.Errorf("cannot compare null value")
		}
		return &FloatComparable{val.Float64}, nil
	case sql.NullTime:
		if !val.Valid {
			return nil, fmt.Errorf("cannot compare null value")
		}
		return &TimeComparable{val.Time}, nil
	default:
		return nil, fmt.Errorf("unsupported type for comparison: %T", v)
	}
}
