package db

import (
	"math"
	"regexp"
	"strconv"
	"strings"

	"isodb/sql"
)

type colInfo struct {
	table, name, kind string
}

type layout struct {
	cols   []colInfo
	byName map[string][]int
}

func newLayout(cols []colInfo) *layout {
	l := &layout{cols: cols, byName: map[string][]int{}}
	for i, c := range cols {
		l.byName[c.name] = append(l.byName[c.name], i)
	}
	return l
}

// env is the evaluation context: the current row, the enclosing query's row for correlated
// subqueries, and the group being aggregated.
type env struct {
	l      *layout
	row    []Value
	outer  *env
	group  [][]Value
	s      *Session
	params []Value
	aggs   map[*sql.Func]Value
}

var aggregates = map[string]bool{"count": true, "sum": true, "avg": true, "min": true, "max": true, "total": true, "string_agg": true, "bool_and": true, "bool_or": true, "every": true, "array_agg": true, "group_concat": true}

func (e *env) resolve(c *sql.Col) (*env, int, error) {
	for x := e; x != nil; x = x.outer {
		if x.l == nil {
			continue
		}
		idx := -1
		for _, i := range x.l.byName[c.Name] {
			if c.Table != "" && x.l.cols[i].table != c.Table {
				continue
			}
			if idx >= 0 && c.Table == "" {
				return nil, 0, errf(codeAmbiguous, "column reference \"%s\" is ambiguous", c.Name)
			}
			if idx < 0 {
				idx = i
			}
		}
		if idx >= 0 {
			return x, idx, nil
		}
	}
	if c.Table != "" {
		return nil, 0, errf(codeUndefCol, "column %s.%s does not exist", c.Table, c.Name)
	}
	return nil, 0, errf(codeUndefCol, "column \"%s\" does not exist", c.Name)
}

func truth(v Value) (bool, bool, error) {
	if v == nil {
		return false, true, nil
	}
	b, err := toBool(v)
	return b, false, err
}

func (e *env) eval(x sql.Expr) (Value, error) {
	switch x := x.(type) {
	case *sql.Lit:
		return x.V, nil
	case *sql.Col:
		o, i, err := e.resolve(x)
		if err != nil {
			return nil, err
		}
		if o.row == nil {
			return nil, nil
		}
		return o.row[i], nil
	case *sql.Param:
		if x.N < 1 || x.N > len(e.params) {
			return nil, errf("08P01", "there is no parameter $%d", x.N)
		}
		return e.params[x.N-1], nil
	case *sql.Unary:
		v, err := e.eval(x.X)
		if err != nil || v == nil {
			return nil, err
		}
		if x.Op == "not" {
			b, err := toBool(v)
			return !b, err
		}
		switch n := v.(type) {
		case int64:
			return -n, nil
		case float64:
			return -n, nil
		case string:
			f, err := toFloat(n)
			return -f, err
		}
		return nil, errf(codeType, "operator does not exist: - %s", typeOfValue(v))
	case *sql.Binary:
		return e.binary(x)
	case *sql.IsNull:
		v, err := e.eval(x.X)
		return (v == nil) != x.Not, err
	case *sql.Between:
		v, err := e.eval(x.X)
		if err != nil {
			return nil, err
		}
		lo, err := e.eval(x.Lo)
		if err != nil {
			return nil, err
		}
		hi, err := e.eval(x.Hi)
		if err != nil {
			return nil, err
		}
		a, err := cmpOp(">=", v, lo)
		if err != nil {
			return nil, err
		}
		b, err := cmpOp("<=", v, hi)
		if err != nil {
			return nil, err
		}
		r := and3(a, b)
		if x.Not {
			return not3(r), nil
		}
		return r, nil
	case *sql.In:
		v, err := e.eval(x.X)
		if err != nil {
			return nil, err
		}
		var list []Value
		if x.Sub != nil {
			sr, err := e.s.subquery(x.Sub, e)
			if err != nil {
				return nil, err
			}
			if len(sr.res.cols) != 1 {
				return nil, errf(codeSyntax, "subquery has too many columns")
			}
			if sr.set != nil {
				if len(sr.res.rows) == 0 {
					return x.Not, nil
				}
				if v == nil {
					return nil, nil
				}
				var r Value = sr.set[key([]Value{v})]
				if r == false && sr.hasNull {
					r = nil
				}
				if r == false {
					if _, isStr := v.(string); isStr || sr.mixed() {
						goto slow
					}
				}
				if x.Not {
					return not3(r), nil
				}
				return r, nil
			}
		slow:
			for _, r := range sr.res.rows {
				list = append(list, r[0])
			}
		} else {
			for _, it := range x.List {
				w, err := e.eval(it)
				if err != nil {
					return nil, err
				}
				list = append(list, w)
			}
		}
		if len(list) == 0 {
			return x.Not, nil
		}
		if v == nil {
			return nil, nil
		}
		var r Value = false
		for _, w := range list {
			eq, err := cmpOp("=", v, w)
			if err != nil {
				return nil, err
			}
			r = or3(r, eq)
			if r == true {
				break
			}
		}
		if x.Not {
			return not3(r), nil
		}
		return r, nil
	case *sql.Exists:
		sr, err := e.s.subquery(x.Sub, e)
		if err != nil {
			return nil, err
		}
		return (len(sr.res.rows) > 0) != x.Not, nil
	case *sql.Subquery:
		sr, err := e.s.subquery(x.Sel, e)
		if err != nil {
			return nil, err
		}
		res := sr.res
		if len(res.cols) != 1 {
			return nil, errf(codeSyntax, "subquery must return only one column")
		}
		if len(res.rows) > 1 {
			return nil, errf(codeCardinality, "more than one row returned by a subquery used as an expression")
		}
		if len(res.rows) == 0 {
			return nil, nil
		}
		return res.rows[0][0], nil
	case *sql.Case:
		var op Value
		if x.Operand != nil {
			var err error
			if op, err = e.eval(x.Operand); err != nil {
				return nil, err
			}
		}
		for _, w := range x.Whens {
			c, err := e.eval(w.Cond)
			if err != nil {
				return nil, err
			}
			if x.Operand != nil {
				if c, err = cmpOp("=", op, c); err != nil {
					return nil, err
				}
			}
			if b, null, err := truth(c); err != nil {
				return nil, err
			} else if b && !null {
				return e.eval(w.Then)
			}
		}
		if x.Else != nil {
			return e.eval(x.Else)
		}
		return nil, nil
	case *sql.Cast:
		v, err := e.eval(x.X)
		if err != nil {
			return nil, err
		}
		return coerce(v, kind(x.Type))
	case *sql.Like:
		v, err := e.eval(x.X)
		if err != nil {
			return nil, err
		}
		p, err := e.eval(x.Pattern)
		if err != nil || v == nil || p == nil {
			return nil, err
		}
		re, err := likeRegexp(text(p), x.ILike)
		if err != nil {
			return nil, err
		}
		return re.MatchString(text(v)) != x.Not, nil
	case *sql.Func:
		return e.call(x)
	}
	return nil, errf(codeFeature, "unsupported expression %T", x)
}

var likeCache = map[string]*regexp.Regexp{}

func likeRegexp(p string, icase bool) (*regexp.Regexp, error) {
	k := p
	if icase {
		k = "i" + p
	}
	if re, ok := likeCache[k]; ok {
		return re, nil
	}
	var b strings.Builder
	if icase {
		b.WriteString("(?is)^")
	} else {
		b.WriteString("(?s)^")
	}
	for i := 0; i < len(p); i++ {
		switch c := p[i]; c {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		case '\\':
			if i+1 < len(p) {
				i++
				b.WriteString(regexp.QuoteMeta(string(p[i])))
			}
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return nil, errf(codeBadInput, "invalid pattern")
	}
	if len(likeCache) < 1000 {
		likeCache[k] = re
	}
	return re, nil
}

func not3(v Value) Value {
	if v == nil {
		return nil
	}
	return !v.(bool)
}

func and3(a, b Value) Value {
	if a == false || b == false {
		return false
	}
	if a == nil || b == nil {
		return nil
	}
	return true
}

func or3(a, b Value) Value {
	if a == true || b == true {
		return true
	}
	if a == nil || b == nil {
		return nil
	}
	return false
}

func cmpOp(op string, a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	c, err := compare(a, b)
	if err != nil {
		return nil, err
	}
	switch op {
	case "=":
		return c == 0, nil
	case "<>":
		return c != 0, nil
	case "<":
		return c < 0, nil
	case "<=":
		return c <= 0, nil
	case ">":
		return c > 0, nil
	}
	return c >= 0, nil
}

func (e *env) binary(x *sql.Binary) (Value, error) {
	if x.Op == "and" || x.Op == "or" {
		a, err := e.eval(x.L)
		if err != nil {
			return nil, err
		}
		ab, an, err := truth(a)
		if err != nil {
			return nil, err
		}
		if x.Op == "and" && !an && !ab {
			return false, nil
		}
		if x.Op == "or" && !an && ab {
			return true, nil
		}
		b, err := e.eval(x.R)
		if err != nil {
			return nil, err
		}
		bb, bn, err := truth(b)
		if err != nil {
			return nil, err
		}
		var av, bv Value = ab, bb
		if an {
			av = nil
		}
		if bn {
			bv = nil
		}
		if x.Op == "and" {
			return and3(av, bv), nil
		}
		return or3(av, bv), nil
	}
	a, err := e.eval(x.L)
	if err != nil {
		return nil, err
	}
	b, err := e.eval(x.R)
	if err != nil {
		return nil, err
	}
	switch x.Op {
	case "=", "<>", "<", "<=", ">", ">=":
		return cmpOp(x.Op, a, b)
	case "is distinct", "is not distinct":
		same := a == nil && b == nil
		if a != nil && b != nil {
			c, err := compare(a, b)
			if err != nil {
				return nil, err
			}
			same = c == 0
		}
		return same == (x.Op == "is not distinct"), nil
	case "||":
		if a == nil || b == nil {
			return nil, nil
		}
		return text(a) + text(b), nil
	}
	if a == nil || b == nil {
		return nil, nil
	}
	return arith(x.Op, a, b)
}

func arith(op string, a, b Value) (Value, error) {
	ai, aInt := a.(int64)
	bi, bInt := b.(int64)
	if s, ok := a.(string); ok {
		if n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
			ai, aInt = n, true
		}
	}
	if s, ok := b.(string); ok {
		if n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
			bi, bInt = n, true
		}
	}
	if aInt && bInt {
		switch op {
		case "+":
			r := ai + bi
			if (r > ai) != (bi > 0) {
				return nil, errf(codeOutOfRange, "bigint out of range")
			}
			return r, nil
		case "-":
			r := ai - bi
			if (r < ai) != (bi > 0) {
				return nil, errf(codeOutOfRange, "bigint out of range")
			}
			return r, nil
		case "*":
			if ai != 0 && bi != 0 {
				r := ai * bi
				if r/bi != ai || ai == -1 && bi == math.MinInt64 || bi == -1 && ai == math.MinInt64 {
					return nil, errf(codeOutOfRange, "bigint out of range")
				}
				return r, nil
			}
			return int64(0), nil
		case "/":
			if bi == 0 {
				return nil, errf(codeDivZero, "division by zero")
			}
			if ai == math.MinInt64 && bi == -1 {
				return nil, errf(codeOutOfRange, "bigint out of range")
			}
			return ai / bi, nil
		case "%":
			if bi == 0 {
				return nil, errf(codeDivZero, "division by zero")
			}
			if bi == -1 {
				return int64(0), nil
			}
			return ai % bi, nil
		}
	}
	af, err := toFloat(a)
	if err != nil {
		return nil, err
	}
	bf, err := toFloat(b)
	if err != nil {
		return nil, err
	}
	switch op {
	case "+":
		return af + bf, nil
	case "-":
		return af - bf, nil
	case "*":
		return af * bf, nil
	case "/":
		if bf == 0 {
			return nil, errf(codeDivZero, "division by zero")
		}
		return af / bf, nil
	case "%":
		if bf == 0 {
			return nil, errf(codeDivZero, "division by zero")
		}
		return math.Mod(af, bf), nil
	}
	return nil, errf(codeFeature, "unsupported operator %s", op)
}

func (e *env) call(f *sql.Func) (Value, error) {
	if f.Name == "$any" || f.Name == "$all" {
		op := f.Args[0].(*sql.Lit).V.(string)
		v, err := e.eval(f.Args[1])
		if err != nil {
			return nil, err
		}
		sr, err := e.s.subquery(f.Args[2].(*sql.Subquery).Sel, e)
		if err != nil {
			return nil, err
		}
		res := sr.res
		var r Value = f.Name == "$all"
		for _, row := range res.rows {
			c, err := cmpOp(op, v, row[0])
			if err != nil {
				return nil, err
			}
			if f.Name == "$any" {
				r = or3(r, c)
			} else {
				r = and3(r, c)
			}
		}
		return r, nil
	}
	if aggregates[f.Name] {
		if e.aggs != nil {
			if v, ok := e.aggs[f]; ok {
				return v, nil
			}
		}
		if e.group == nil {
			return nil, errf(codeGrouping, "aggregate function %s used outside of a grouped context", f.Name)
		}
		return e.aggregate(f)
	}
	args := make([]Value, len(f.Args))
	for i, a := range f.Args {
		v, err := e.eval(a)
		if err != nil {
			return nil, err
		}
		args[i] = v
	}
	switch f.Name {
	case "coalesce", "ifnull":
		for _, a := range args {
			if a != nil {
				return a, nil
			}
		}
		return nil, nil
	case "nullif":
		if len(args) != 2 {
			break
		}
		if args[0] != nil && args[1] != nil {
			if c, err := compare(args[0], args[1]); err == nil && c == 0 {
				return nil, nil
			}
		}
		return args[0], nil
	case "abs":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		switch v := args[0].(type) {
		case int64:
			if v < 0 {
				return -v, nil
			}
			return v, nil
		}
		x, err := toFloat(args[0])
		return math.Abs(x), err
	case "length", "char_length", "character_length":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		return int64(len([]rune(text(args[0])))), nil
	case "upper":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		return strings.ToUpper(text(args[0])), nil
	case "lower":
		if len(args) != 1 || args[0] == nil {
			return nil, nil
		}
		return strings.ToLower(text(args[0])), nil
	case "round", "floor", "ceil", "ceiling", "trunc", "sqrt":
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		if n, ok := args[0].(int64); ok && f.Name != "sqrt" {
			return n, nil
		}
		x, err := toFloat(args[0])
		if err != nil {
			return nil, err
		}
		scale := 1.0
		if len(args) > 1 {
			d, err := toInt(args[1])
			if err != nil {
				return nil, err
			}
			scale = math.Pow(10, float64(d))
		}
		switch f.Name {
		case "round":
			return math.Round(x*scale) / scale, nil
		case "floor":
			return math.Floor(x), nil
		case "ceil", "ceiling":
			return math.Ceil(x), nil
		case "trunc":
			return math.Trunc(x*scale) / scale, nil
		}
		return math.Sqrt(x), nil
	case "concat":
		var b strings.Builder
		for _, a := range args {
			b.WriteString(text(a))
		}
		return b.String(), nil
	case "substr", "substring":
		if len(args) < 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		r := []rune(text(args[0]))
		from, err := toInt(args[1])
		if err != nil {
			return nil, err
		}
		n := int64(len(r)) + 1
		if len(args) > 2 {
			if args[2] == nil {
				return nil, nil
			}
			if n, err = toInt(args[2]); err != nil {
				return nil, err
			}
			n += from
		} else {
			n = int64(len(r)) + 1
		}
		lo, hi := max64(from, 1), min64(n, int64(len(r))+1)
		if lo >= hi {
			return "", nil
		}
		return string(r[lo-1 : hi-1]), nil
	case "greatest", "least":
		var best Value
		for _, a := range args {
			if a == nil {
				continue
			}
			if best == nil {
				best = a
				continue
			}
			c, err := compare(a, best)
			if err != nil {
				return nil, err
			}
			if f.Name == "greatest" && c > 0 || f.Name == "least" && c < 0 {
				best = a
			}
		}
		return best, nil
	case "version":
		return "PostgreSQL 14.0 (isodb)", nil
	case "current_database":
		return "isodb", nil
	case "current_schema":
		return "public", nil
	case "current_user", "session_user", "user":
		return e.s.user, nil
	case "now", "current_timestamp", "transaction_timestamp", "statement_timestamp", "clock_timestamp":
		return e.s.now(), nil
	case "random":
		return e.s.rand.Float64(), nil
	case "$default":
		return nil, errf(codeSyntax, "DEFAULT is not allowed in this context")
	}
	return nil, errf("42883", "function %s does not exist", f.Name)
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (e *env) aggregate(f *sql.Func) (Value, error) {
	if f.Name == "count" && f.Star {
		return int64(len(e.group)), nil
	}
	if len(f.Args) == 0 {
		return nil, errf("42883", "function %s() does not exist", f.Name)
	}
	var vals []Value
	seen := map[string]bool{}
	sub := &env{l: e.l, outer: e.outer, s: e.s, params: e.params}
	for _, r := range e.group {
		sub.row = r
		v, err := sub.eval(f.Args[0])
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		if f.Distinct {
			k := key([]Value{v})
			if seen[k] {
				continue
			}
			seen[k] = true
		}
		vals = append(vals, v)
	}
	switch f.Name {
	case "count":
		return int64(len(vals)), nil
	case "sum", "total":
		if len(vals) == 0 {
			if f.Name == "total" {
				return 0.0, nil
			}
			return nil, nil
		}
		var acc Value = int64(0)
		for _, v := range vals {
			if s, ok := v.(string); ok {
				f, err := toFloat(s)
				if err != nil {
					return nil, err
				}
				v = f
			}
			var err error
			if acc, err = arith("+", acc, v); err != nil {
				return nil, err
			}
		}
		return acc, nil
	case "avg":
		if len(vals) == 0 {
			return nil, nil
		}
		s := 0.0
		for _, v := range vals {
			x, err := toFloat(v)
			if err != nil {
				return nil, err
			}
			s += x
		}
		return s / float64(len(vals)), nil
	case "min", "max":
		var best Value
		for _, v := range vals {
			if best == nil {
				best = v
				continue
			}
			c, err := compare(v, best)
			if err != nil {
				return nil, err
			}
			if f.Name == "min" && c < 0 || f.Name == "max" && c > 0 {
				best = v
			}
		}
		return best, nil
	case "bool_and", "every", "bool_or":
		if len(vals) == 0 {
			return nil, nil
		}
		r := f.Name != "bool_or"
		for _, v := range vals {
			b, err := toBool(v)
			if err != nil {
				return nil, err
			}
			if f.Name == "bool_or" {
				r = r || b
			} else {
				r = r && b
			}
		}
		return r, nil
	case "string_agg", "group_concat":
		if len(vals) == 0 {
			return nil, nil
		}
		sep := ","
		if len(f.Args) > 1 {
			s, err := e.eval(f.Args[1])
			if err != nil {
				return nil, err
			}
			sep = text(s)
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = text(v)
		}
		return strings.Join(parts, sep), nil
	}
	return nil, errf(codeFeature, "aggregate %s is not supported", f.Name)
}

// walk calls fn for every expression node, not descending into subqueries.
func walk(x sql.Expr, fn func(sql.Expr)) {
	if x == nil {
		return
	}
	fn(x)
	switch x := x.(type) {
	case *sql.Unary:
		walk(x.X, fn)
	case *sql.Binary:
		walk(x.L, fn)
		walk(x.R, fn)
	case *sql.IsNull:
		walk(x.X, fn)
	case *sql.Between:
		walk(x.X, fn)
		walk(x.Lo, fn)
		walk(x.Hi, fn)
	case *sql.In:
		walk(x.X, fn)
		for _, i := range x.List {
			walk(i, fn)
		}
	case *sql.Case:
		walk(x.Operand, fn)
		for _, w := range x.Whens {
			walk(w.Cond, fn)
			walk(w.Then, fn)
		}
		walk(x.Else, fn)
	case *sql.Cast:
		walk(x.X, fn)
	case *sql.Like:
		walk(x.X, fn)
		walk(x.Pattern, fn)
	case *sql.Func:
		for _, a := range x.Args {
			walk(a, fn)
		}
	}
}

func hasAggregate(x sql.Expr) bool {
	found := false
	walk(x, func(e sql.Expr) {
		if f, ok := e.(*sql.Func); ok && aggregates[f.Name] {
			found = true
		}
	})
	return found
}

func hasSubquery(x sql.Expr) bool {
	found := false
	walk(x, func(e sql.Expr) {
		switch y := e.(type) {
		case *sql.Subquery, *sql.Exists:
			found = true
		case *sql.In:
			if y.Sub != nil {
				found = true
			}
		case *sql.Func:
			if y.Name == "$any" || y.Name == "$all" {
				found = true
			}
		}
	})
	return found
}

func (sr *subResult) mixed() bool {
	for _, r := range sr.res.rows {
		if _, ok := r[0].(string); ok {
			return true
		}
	}
	return false
}

// subquery runs a subquery, or reuses its result when it does not read the outer query's row:
// such a subquery gives the same answer for every row of one statement.
func (s *Session) subquery(sel *sql.Select, outer *env) (*subResult, error) {
	if s.corr == nil {
		s.corr = map[*sql.Select]bool{}
	}
	corr, known := s.corr[sel]
	if !known {
		corr = s.correlated(sel, nil)
		s.corr[sel] = corr
	}
	if !corr {
		if sr := s.subs[sel]; sr != nil {
			return sr, nil
		}
	}
	res, err := s.query(sel, outer)
	if err != nil {
		return nil, err
	}
	sr := &subResult{res: res}
	if !corr {
		if len(res.cols) == 1 {
			sr.set = map[string]bool{}
			for _, r := range res.rows {
				if r[0] == nil {
					sr.hasNull = true
				} else {
					sr.set[key(r[:1])] = true
				}
			}
		}
		if s.subs == nil {
			s.subs = map[*sql.Select]*subResult{}
		}
		s.subs[sel] = sr
	}
	return sr, nil
}

// correlated reports whether a subquery reads any column that its own FROM clauses do not provide.
func (s *Session) correlated(sel *sql.Select, scopes []*layout) bool {
	for cur := sel; cur != nil; cur = cur.Right {
		var cols []colInfo
		for _, tr := range cur.From {
			alias := tr.Alias
			if tr.Sub != nil {
				if s.correlated(tr.Sub, scopes) {
					return true
				}
				names, kinds, err := s.describe(tr.Sub)
				if err != nil {
					return true
				}
				for i, n := range names {
					cols = append(cols, colInfo{alias, n, kinds[i]})
				}
				continue
			}
			if alias == "" {
				alias = tr.Name
			}
			if tb := s.db.tables[tr.Name]; tb != nil {
				cols = append(cols, s.tableCols(tb, alias)...)
			} else if v := s.db.views[tr.Name]; v != nil {
				names, kinds, err := s.describe(v)
				if err != nil {
					return true
				}
				for i, n := range names {
					cols = append(cols, colInfo{alias, n, kinds[i]})
				}
			} else {
				return true
			}
		}
		inner := append([]*layout{newLayout(cols)}, scopes...)
		var exprs []sql.Expr
		for _, it := range cur.Items {
			exprs = append(exprs, it.Expr)
		}
		exprs = append(exprs, cur.Where, cur.Having, cur.Limit, cur.Offset)
		exprs = append(exprs, cur.GroupBy...)
		for _, tr := range cur.From {
			exprs = append(exprs, tr.On)
		}
		for _, o := range cur.OrderBy {
			exprs = append(exprs, o.Expr)
		}
		for _, x := range exprs {
			bad := false
			walkAll(x, func(e sql.Expr) {
				switch y := e.(type) {
				case *sql.Col:
					if !resolvable(y, inner) {
						bad = true
					}
				case *sql.Param:
					bad = true
				case *sql.Func:
					if y.Name == "random" || y.Name == "now" {
						bad = true
					}
				}
			}, func(sub *sql.Select) {
				if s.correlated(sub, inner) {
					bad = true
				}
			})
			if bad {
				return true
			}
		}
	}
	return false
}

func resolvable(c *sql.Col, scopes []*layout) bool {
	if c.Table == "excluded" {
		return false
	}
	for _, l := range scopes[:1] {
		for _, i := range l.byName[c.Name] {
			if c.Table == "" || l.cols[i].table == c.Table {
				return true
			}
		}
	}
	return false
}

// walkAll is walk that also hands nested subqueries to sub.
func walkAll(x sql.Expr, fn func(sql.Expr), sub func(*sql.Select)) {
	walk(x, func(e sql.Expr) {
		fn(e)
		switch y := e.(type) {
		case *sql.Subquery:
			sub(y.Sel)
		case *sql.Exists:
			sub(y.Sub)
		case *sql.In:
			if y.Sub != nil {
				sub(y.Sub)
			}
		case *sql.Func:
			if y.Name == "$any" || y.Name == "$all" {
				sub(y.Args[2].(*sql.Subquery).Sel)
			}
		}
	})
}
