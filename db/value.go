package db

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Value is nil, int64, float64, string or bool.
type Value = interface{}

// Error carries a Postgres SQLSTATE code.
type Error struct {
	Code, Msg string
}

func (e *Error) Error() string { return e.Msg }

func errf(code, f string, a ...interface{}) *Error { return &Error{code, fmt.Sprintf(f, a...)} }

const (
	codeSerialization = "40001"
	codeDeadlock      = "40P01"
	codeUndefTable    = "42P01"
	codeUndefCol      = "42703"
	codeSyntax        = "42601"
	codeUnique        = "23505"
	codeNotNull       = "23502"
	codeDivZero       = "22012"
	codeBadInput      = "22P02"
	codeFailedTx      = "25P02"
	codeDupTable      = "42P07"
	codeAmbiguous     = "42702"
	codeOutOfRange    = "22003"
	codeFeature       = "0A000"
	codeCardinality   = "21000"
	codeGrouping      = "42803"
	codeType          = "42804"
)

func kind(t string) string {
	t = strings.TrimSuffix(t, "[]")
	switch t {
	case "int", "integer", "int4", "int8", "bigint", "smallint", "int2", "tinyint", "mediumint":
		return "int"
	case "real", "float", "float4", "float8", "double", "numeric", "decimal":
		return "float"
	case "bool", "boolean":
		return "bool"
	}
	return "text"
}

// OID of the Postgres type used to describe a column of this kind.
func oid(k string) int32 {
	switch k {
	case "int":
		return 20
	case "float":
		return 701
	case "bool":
		return 16
	}
	return 25
}

func typeOfValue(v Value) string {
	switch v.(type) {
	case int64:
		return "int"
	case float64:
		return "float"
	case bool:
		return "bool"
	case string:
		return "text"
	}
	return ""
}

func toInt(v Value) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case float64:
		if math.IsNaN(x) || x > math.MaxInt64 || x < math.MinInt64 {
			return 0, errf(codeOutOfRange, "integer out of range")
		}
		return int64(math.Round(x)), nil
	case bool:
		if x {
			return 1, nil
		}
		return 0, nil
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		if err != nil {
			return 0, errf(codeBadInput, "invalid input syntax for type integer: \"%s\"", x)
		}
		return n, nil
	}
	return 0, errf(codeType, "cannot convert to integer")
}

func toFloat(v Value) (float64, error) {
	switch x := v.(type) {
	case int64:
		return float64(x), nil
	case float64:
		return x, nil
	case string:
		s := strings.TrimSpace(x)
		switch strings.ToLower(s) {
		case "nan":
			return math.NaN(), nil
		case "infinity", "inf", "+infinity":
			return math.Inf(1), nil
		case "-infinity", "-inf":
			return math.Inf(-1), nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, errf(codeBadInput, "invalid input syntax for type double precision: \"%s\"", x)
		}
		return f, nil
	case bool:
		if x {
			return 1, nil
		}
		return 0, nil
	}
	return 0, errf(codeType, "cannot convert to double precision")
}

func toBool(v Value) (bool, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case int64:
		return x != 0, nil
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "t", "true", "yes", "on", "1", "y":
			return true, nil
		case "f", "false", "no", "off", "0", "n":
			return false, nil
		}
		return false, errf(codeBadInput, "invalid input syntax for type boolean: \"%s\"", x)
	}
	return false, errf(codeType, "cannot convert to boolean")
}

// coerce converts v to a column kind.
func coerce(v Value, k string) (Value, error) {
	if v == nil {
		return nil, nil
	}
	switch k {
	case "int":
		return toInt(v)
	case "float":
		return toFloat(v)
	case "bool":
		return toBool(v)
	}
	return text(v), nil
}

// text renders a value the way Postgres prints it in text format.
func text(v Value) string {
	switch x := v.(type) {
	case nil:
		return ""
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		switch {
		case math.IsNaN(x):
			return "NaN"
		case math.IsInf(x, 1):
			return "Infinity"
		case math.IsInf(x, -1):
			return "-Infinity"
		}
		return strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		if x {
			return "t"
		}
		return "f"
	case string:
		return x
	}
	return fmt.Sprint(v)
}

// compare returns -1, 0, 1. Both values must be non-nil.
func compare(a, b Value) (int, error) {
	switch x := a.(type) {
	case int64:
		switch y := b.(type) {
		case int64:
			return cmp3(x < y, x > y), nil
		case float64:
			return cmpf(float64(x), y), nil
		case string:
			if n, err := toInt(y); err == nil {
				return cmp3(x < n, x > n), nil
			}
			f, err := toFloat(y)
			if err != nil {
				return 0, err
			}
			return cmpf(float64(x), f), nil
		case bool:
			return compare(x, mustInt(y))
		}
	case float64:
		switch y := b.(type) {
		case int64:
			return cmpf(x, float64(y)), nil
		case float64:
			return cmpf(x, y), nil
		case string:
			f, err := toFloat(y)
			if err != nil {
				return 0, err
			}
			return cmpf(x, f), nil
		}
	case string:
		switch y := b.(type) {
		case string:
			return strings.Compare(x, y), nil
		case int64, float64, bool:
			c, err := compare(b, a)
			return -c, err
		}
	case bool:
		switch y := b.(type) {
		case bool:
			return cmp3(!x && y, x && !y), nil
		case string:
			yb, err := toBool(y)
			if err != nil {
				return 0, err
			}
			return compare(x, yb)
		case int64:
			return compare(mustInt(x), y)
		}
	}
	return 0, errf(codeType, "cannot compare %s with %s", typeOfValue(a), typeOfValue(b))
}

func mustInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func cmp3(lt, gt bool) int {
	if lt {
		return -1
	}
	if gt {
		return 1
	}
	return 0
}

// Postgres orders NaN above every other float
func cmpf(x, y float64) int {
	xn, yn := math.IsNaN(x), math.IsNaN(y)
	switch {
	case xn && yn:
		return 0
	case xn:
		return 1
	case yn:
		return -1
	}
	return cmp3(x < y, x > y)
}

// key encodes values so that equal values give equal strings (1 and 1.0 compare equal in SQL).
func key(vals []Value) string {
	var b strings.Builder
	for _, v := range vals {
		switch x := v.(type) {
		case nil:
			b.WriteByte(0)
		case int64:
			intKey(&b, x)
		case float64:
			if x == math.Trunc(x) && math.Abs(x) < 1e18 {
				intKey(&b, int64(x))
			} else {
				b.WriteByte(2)
				b.WriteString(strconv.FormatFloat(x, 'g', -1, 64))
			}
		case string:
			b.WriteByte(3)
			b.WriteString(x)
		case bool:
			b.WriteByte(4)
			if x {
				b.WriteByte('t')
			} else {
				b.WriteByte('f')
			}
		}
		b.WriteByte(0xff)
	}
	return b.String()
}

// intKey writes an integer as 8 big-endian bytes with the sign flipped, so keys sort numerically
func intKey(b *strings.Builder, x int64) {
	var buf [9]byte
	buf[0] = 1
	binary.BigEndian.PutUint64(buf[1:], uint64(x)^1<<63)
	b.Write(buf[:])
}

// binary encoding used by the write-ahead log

func putValue(b []byte, v Value) []byte {
	switch x := v.(type) {
	case nil:
		return append(b, 0)
	case int64:
		b = append(b, 1)
		return binary.LittleEndian.AppendUint64(b, uint64(x))
	case float64:
		b = append(b, 2)
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(x))
	case string:
		b = append(b, 3)
		b = binary.AppendUvarint(b, uint64(len(x)))
		return append(b, x...)
	case bool:
		if x {
			return append(b, 4, 1)
		}
		return append(b, 4, 0)
	}
	panic(fmt.Sprintf("unknown value %T", v))
}

type reader struct {
	b   []byte
	err bool
}

func (r *reader) byte() byte {
	if len(r.b) < 1 {
		r.err = true
		return 0
	}
	c := r.b[0]
	r.b = r.b[1:]
	return c
}

func (r *reader) u64() uint64 {
	if len(r.b) < 8 {
		r.err = true
		return 0
	}
	v := binary.LittleEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v
}

func (r *reader) uvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = true
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) str() string {
	n := r.uvarint()
	if uint64(len(r.b)) < n {
		r.err = true
		return ""
	}
	s := string(r.b[:n])
	r.b = r.b[n:]
	return s
}

func (r *reader) value() Value {
	switch r.byte() {
	case 0:
		return nil
	case 1:
		return int64(r.u64())
	case 2:
		return math.Float64frombits(r.u64())
	case 3:
		return r.str()
	case 4:
		return r.byte() == 1
	}
	r.err = true
	return nil
}

// Text renders a value in Postgres text format.
func Text(v Value) string { return text(v) }

// Coerce converts a value to a column kind: int, float, bool or text.
func Coerce(v Value, k string) (Value, error) { return coerce(v, k) }
