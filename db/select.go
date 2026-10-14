package db

import (
	"sort"

	"isodb/sql"
)

type result struct {
	cols  []string
	kinds []string
	rows  [][]Value
}

type source struct {
	cols  []colInfo
	table *Table
	rows  [][]Value
	refs  []*Row
	sub   bool
	join  string
	on    sql.Expr
	pk    sql.Expr // equality on the primary key column: fetch by key instead of scanning
	pkCol int
}

func (s *Session) tableCols(tb *Table, alias string) []colInfo {
	cols := make([]colInfo, len(tb.Cols))
	for i, c := range tb.Cols {
		cols[i] = colInfo{alias, c.Name, c.Kind}
	}
	return cols
}

// visibleRows scans a table under the current transaction.
func (s *Session) visibleRows(tb *Table) ([][]Value, []*Row, error) {
	db := s.db
	db.scan(s.tx, tb)
	var rows [][]Value
	var refs []*Row
	for _, r := range tb.order {
		if len(r.vers) == 0 {
			continue
		}
		vals, err := db.read(s.tx, tb, r)
		if err != nil {
			return nil, nil, err
		}
		if vals != nil {
			rows = append(rows, vals)
			refs = append(refs, r)
		}
	}
	return rows, refs, nil
}

func (s *Session) lookup(tb *Table, k string) ([]Value, *Row, error) {
	r := s.db.row(tb, k)
	vals, err := s.db.read(s.tx, tb, r)
	return vals, r, err
}

// refsOf returns the sources an expression reads; columns it cannot place belong to an outer query.
func refsOf(x sql.Expr, l *layout, starts []int) (map[int]bool, bool) {
	refs := map[int]bool{}
	walk(x, func(e sql.Expr) {
		c, isCol := e.(*sql.Col)
		if !isCol {
			return
		}
		for _, i := range l.byName[c.Name] {
			if c.Table != "" && l.cols[i].table != c.Table {
				continue
			}
			for si := len(starts) - 1; si >= 0; si-- {
				if i >= starts[si] {
					refs[si] = true
					break
				}
			}
		}
	})
	return refs, true
}

func splitAnd(x sql.Expr, out []sql.Expr) []sql.Expr {
	if b, ok := x.(*sql.Binary); ok && b.Op == "and" {
		return splitAnd(b.R, splitAnd(b.L, out))
	}
	if x != nil {
		out = append(out, x)
	}
	return out
}

func (s *Session) query(sel *sql.Select, outer *env) (*result, error) {
	if sel.Right == nil {
		return s.core(sel, outer, sel.OrderBy, true)
	}
	res, err := s.core(sel, outer, nil, false)
	if err != nil {
		return nil, err
	}
	for cur := sel; cur.Right != nil; cur = cur.Right {
		r, err := s.core(cur.Right, outer, nil, false)
		if err != nil {
			return nil, err
		}
		if len(r.cols) != len(res.cols) {
			return nil, errf(codeSyntax, "each %s query must have the same number of columns", cur.Op)
		}
		res.rows = setOp(cur.Op, cur.All, res.rows, r.rows)
	}
	if len(sel.OrderBy) > 0 {
		keys := make([][]Value, len(res.rows))
		for i, row := range res.rows {
			for _, o := range sel.OrderBy {
				pos := outputPos(o.Expr, res.cols)
				if pos < 0 {
					return nil, errf(codeFeature, "ORDER BY on a UNION must name an output column")
				}
				keys[i] = append(keys[i], row[pos])
			}
		}
		sortRows(res.rows, keys, sel.OrderBy)
	}
	return s.limit(sel, outer, res)
}

func setOp(op string, all bool, a, b [][]Value) [][]Value {
	switch op {
	case "union":
		out := append(a, b...)
		if all {
			return out
		}
		return distinct(out)
	case "intersect", "except":
		in := map[string]int{}
		for _, r := range b {
			in[key(r)]++
		}
		var out [][]Value
		for _, r := range a {
			k := key(r)
			if (in[k] > 0) == (op == "intersect") {
				out = append(out, r)
				if all && op == "intersect" {
					in[k]--
				}
			} else if all && op == "except" {
				in[k]--
			}
		}
		if all {
			return out
		}
		return distinct(out)
	}
	return a
}

func distinct(rows [][]Value) [][]Value {
	seen := map[string]bool{}
	out := rows[:0:0]
	for _, r := range rows {
		k := key(r)
		if !seen[k] {
			seen[k] = true
			out = append(out, r)
		}
	}
	return out
}

func outputPos(x sql.Expr, cols []string) int {
	if l, ok := x.(*sql.Lit); ok {
		if n, ok := l.V.(int64); ok && n >= 1 && int(n) <= len(cols) {
			return int(n) - 1
		}
	}
	if c, ok := x.(*sql.Col); ok && c.Table == "" {
		for i, n := range cols {
			if n == c.Name {
				return i
			}
		}
	}
	return -1
}

func sortRows(rows [][]Value, keys [][]Value, order []sql.Order) {
	idx := make([]int, len(rows))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(x, y int) bool {
		a, b := keys[idx[x]], keys[idx[y]]
		for i, o := range order {
			c := 0
			switch {
			case a[i] == nil && b[i] == nil:
			case a[i] == nil:
				c = 1
			case b[i] == nil:
				c = -1
			default:
				c, _ = compare(a[i], b[i])
			}
			if o.Desc {
				c = -c
			}
			if c != 0 {
				return c < 0
			}
		}
		return false
	})
	sorted := make([][]Value, len(rows))
	for i, j := range idx {
		sorted[i] = rows[j]
	}
	copy(rows, sorted)
}

func (s *Session) limit(sel *sql.Select, outer *env, res *result) (*result, error) {
	e := &env{outer: outer, s: s, params: s.params}
	if sel.Offset != nil {
		v, err := e.eval(sel.Offset)
		if err != nil {
			return nil, err
		}
		if v != nil {
			n, err := toInt(v)
			if err != nil {
				return nil, err
			}
			if n < 0 {
				return nil, errf("2201X", "OFFSET must not be negative")
			}
			if int(n) >= len(res.rows) {
				res.rows = nil
			} else {
				res.rows = res.rows[n:]
			}
		}
	}
	if sel.Limit != nil {
		v, err := e.eval(sel.Limit)
		if err != nil {
			return nil, err
		}
		if v != nil {
			n, err := toInt(v)
			if err != nil {
				return nil, err
			}
			if n < 0 {
				return nil, errf("2201W", "LIMIT must not be negative")
			}
			if int(n) < len(res.rows) {
				res.rows = res.rows[:n]
			}
		}
	}
	return res, nil
}

func (s *Session) sources(sel *sql.Select, outer *env) ([]*source, error) {
	var srcs []*source
	for _, tr := range sel.From {
		src := &source{join: tr.Join, on: tr.On, pkCol: -1}
		if tr.Sub != nil {
			r, err := s.query(tr.Sub, outer)
			if err != nil {
				return nil, err
			}
			alias := tr.Alias
			for i, c := range r.cols {
				src.cols = append(src.cols, colInfo{alias, c, r.kinds[i]})
			}
			src.rows, src.sub = r.rows, true
		} else if v := s.db.views[tr.Name]; v != nil && s.db.tables[tr.Name] == nil {
			r, err := s.query(v, nil)
			if err != nil {
				return nil, err
			}
			alias := tr.Alias
			if alias == "" {
				alias = tr.Name
			}
			for i, c := range r.cols {
				src.cols = append(src.cols, colInfo{alias, c, r.kinds[i]})
			}
			src.rows, src.sub = r.rows, true
		} else {
			tb, err := s.db.table(tr.Name)
			if err != nil {
				return nil, err
			}
			alias := tr.Alias
			if alias == "" {
				alias = tr.Name
			}
			src.table, src.cols = tb, s.tableCols(tb, alias)
		}
		srcs = append(srcs, src)
	}
	return srcs, nil
}

func (s *Session) core(sel *sql.Select, outer *env, order []sql.Order, final bool) (*result, error) {
	srcs, err := s.sources(sel, outer)
	if err != nil {
		return nil, err
	}
	var all []colInfo
	starts := make([]int, len(srcs))
	for i, src := range srcs {
		starts[i] = len(all)
		all = append(all, src.cols...)
	}
	l := newLayout(all)
	n := len(srcs)
	hasLeft := false
	for _, src := range srcs {
		if src.join == "left" {
			hasLeft = true
		}
	}
	// a HAVING clause without aggregates is part of WHERE for Postgres
	if sel.Where != nil && neverTrue(sel.Where) || sel.Having != nil && !hasAggregate(sel.Having) && neverTrue(sel.Having) {
		return s.project(sel, l, nil, outer, order, final)
	}
	e := &env{l: l, outer: outer, s: s, params: s.params}
	cur := make([]Value, len(all))
	curRefs := make([]*Row, n)
	test := func(conds []sql.Expr) (bool, error) {
		e.row = cur
		for _, c := range conds {
			v, err := e.eval(c)
			if err != nil {
				return false, err
			}
			b, null, err := truth(v)
			if err != nil {
				return false, err
			}
			if !b || null {
				return false, nil
			}
		}
		return true, nil
	}

	type conj struct {
		x     sql.Expr
		refs  map[int]bool
		final bool
		done  bool
	}
	var conjs []*conj
	for _, c := range splitAnd(sel.Where, nil) {
		cj := &conj{x: c}
		if hasLeft || n == 0 || hasSubquery(c) || hasAggregate(c) {
			cj.final = true
		} else {
			cj.refs, _ = refsOf(c, l, starts)
		}
		conjs = append(conjs, cj)
	}

	// primary key equal to a value that needs no other table: fetch that row instead of scanning
	for _, src := range srcs {
		if src.table == nil || len(src.table.PK) != 1 || hasLeft {
			continue
		}
		pkName := src.table.Cols[src.table.PK[0]].Name
		for _, cj := range conjs {
			b, ok := cj.x.(*sql.Binary)
			if cj.final || !ok || b.Op != "=" {
				continue
			}
			for _, side := range [][2]sql.Expr{{b.L, b.R}, {b.R, b.L}} {
				col, ok := side[0].(*sql.Col)
				if !ok || col.Name != pkName || col.Table != "" && col.Table != src.cols[0].table || col.Table == "" && len(l.byName[col.Name]) > 1 {
					continue
				}
				if refs, _ := refsOf(side[1], l, starts); len(refs) > 0 || hasSubquery(side[1]) {
					continue
				}
				src.pk, src.pkCol = side[1], src.table.PK[0]
				break
			}
			if src.pk != nil {
				break
			}
		}
	}

	// load each table and filter it by the conditions that read only that table
	for i, src := range srcs {
		if src.table != nil {
			if src.pk != nil {
				e.row = cur
				v, err := e.eval(src.pk)
				if err != nil {
					return nil, err
				}
				if v != nil {
					if v, err = coerce(v, src.table.Cols[src.pkCol].Kind); err != nil {
						return nil, err
					}
					vals, r, err := s.lookup(src.table, key([]Value{v}))
					if err != nil {
						return nil, err
					}
					if vals != nil {
						src.rows, src.refs = [][]Value{vals}, []*Row{r}
					}
				}
			} else if src.rows, src.refs, err = s.visibleRows(src.table); err != nil {
				return nil, err
			}
		}
		var mine []sql.Expr
		for _, cj := range conjs {
			if !cj.final && len(cj.refs) == 1 && cj.refs[i] {
				mine = append(mine, cj.x)
				cj.done = true
			}
		}
		if len(mine) == 0 {
			continue
		}
		width := len(src.cols)
		var rows [][]Value
		var refs []*Row
		for ri, r := range src.rows {
			copy(cur[starts[i]:starts[i]+width], r)
			ok, err := test(mine)
			if err != nil {
				return nil, err
			}
			if ok {
				rows = append(rows, r)
				if src.refs != nil {
					refs = append(refs, src.refs[ri])
				}
			}
		}
		src.rows = rows
		if src.refs != nil {
			src.refs = refs
		}
	}

	// join order: FROM order with outer joins, otherwise smallest table first, then follow join conditions
	joinOrder := make([]int, 0, n)
	pos := make([]int, n)
	bound := make([]bool, n)
	for len(joinOrder) < n {
		best, bestScore := -1, 0
		for i := 0; i < n; i++ {
			if bound[i] {
				continue
			}
			if hasLeft {
				best = i
				break
			}
			connected := len(joinOrder) == 0
			for _, cj := range conjs {
				if cj.done || cj.final || !cj.refs[i] {
					continue
				}
				for r := range cj.refs {
					if bound[r] {
						connected = true
					}
				}
			}
			score := len(srcs[i].rows)
			if !connected {
				score += 1 << 40
			}
			if best < 0 || score < bestScore {
				best, bestScore = i, score
			}
		}
		pos[best] = len(joinOrder)
		bound[best] = true
		joinOrder = append(joinOrder, best)
	}
	levels := make([][]sql.Expr, n+1)
	for _, cj := range conjs {
		if cj.done {
			continue
		}
		lv := n
		if !cj.final {
			lv = 0
			for r := range cj.refs {
				if pos[r] > lv {
					lv = pos[r]
				}
			}
		}
		levels[lv] = append(levels[lv], cj.x)
	}
	// hash join: an equality between this table and tables already joined
	type hashJoin struct {
		probe sql.Expr
		index map[string][]int
	}
	hashes := make([]*hashJoin, n)
	for p := 1; p < n && !hasLeft; p++ {
		j := joinOrder[p]
		for _, c := range levels[p] {
			b, ok := c.(*sql.Binary)
			if !ok || b.Op != "=" {
				continue
			}
			for _, side := range [][2]sql.Expr{{b.L, b.R}, {b.R, b.L}} {
				ra, _ := refsOf(side[0], l, starts)
				rb, _ := refsOf(side[1], l, starts)
				if len(ra) != 1 || !ra[j] || len(rb) == 0 || rb[j] {
					continue
				}
				src := srcs[j]
				width := len(src.cols)
				hj := &hashJoin{probe: side[1], index: map[string][]int{}}
				for ri, r := range src.rows {
					copy(cur[starts[j]:starts[j]+width], r)
					e.row = cur
					v, err := e.eval(side[0])
					if err != nil {
						return nil, err
					}
					if v != nil {
						k := key([]Value{v})
						hj.index[k] = append(hj.index[k], ri)
					}
				}
				hashes[p] = hj
				break
			}
			if hashes[p] != nil {
				break
			}
		}
	}

	var rows [][]Value
	var refs [][]*Row
	var loop func(p int) error
	loop = func(p int) error {
		if p == n {
			ok, err := test(levels[n])
			if err != nil || !ok {
				return err
			}
			rows = append(rows, append([]Value(nil), cur...))
			refs = append(refs, append([]*Row(nil), curRefs...))
			return nil
		}
		j := joinOrder[p]
		src := srcs[j]
		width := len(src.cols)
		var cand []int
		if hj := hashes[p]; hj != nil {
			e.row = cur
			v, err := e.eval(hj.probe)
			if err != nil {
				return err
			}
			if v != nil {
				cand = hj.index[key([]Value{v})]
			}
		} else {
			cand = make([]int, len(src.rows))
			for i := range cand {
				cand[i] = i
			}
		}
		conds := levels[p]
		if src.on != nil {
			conds = append([]sql.Expr{src.on}, conds...)
		}
		matched := false
		for _, ri := range cand {
			copy(cur[starts[j]:starts[j]+width], src.rows[ri])
			curRefs[j] = nil
			if src.refs != nil {
				curRefs[j] = src.refs[ri]
			}
			ok, err := test(conds)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			matched = true
			if err := loop(p + 1); err != nil {
				return err
			}
		}
		if !matched && src.join == "left" {
			for k := starts[j]; k < starts[j]+width; k++ {
				cur[k] = nil
			}
			curRefs[j] = nil
			return loop(p + 1)
		}
		return nil
	}
	if err := loop(0); err != nil {
		return nil, err
	}

	if sel.ForUpdate {
		for _, rr := range refs {
			for i, r := range rr {
				if r == nil || srcs[i].table == nil {
					continue
				}
				if err := s.db.lock(s.tx, srcs[i].table, r); err != nil {
					return nil, err
				}
			}
		}
	}

	return s.project(sel, l, rows, outer, order, final)
}

func (s *Session) project(sel *sql.Select, l *layout, rows [][]Value, outer *env, order []sql.Order, final bool) (*result, error) {
	res := &result{}
	var items []sql.SelItem
	for _, it := range sel.Items {
		if !it.Star {
			items = append(items, it)
			continue
		}
		any := false
		for _, c := range l.cols {
			if it.Table == "" || c.table == it.Table {
				items = append(items, sql.SelItem{Expr: &sql.Col{Table: c.table, Name: c.name}, Alias: c.name})
				any = true
			}
		}
		if !any && it.Table != "" {
			return nil, errf(codeUndefTable, "missing FROM-clause entry for table \"%s\"", it.Table)
		}
	}
	for _, it := range items {
		name := it.Alias
		if name == "" {
			switch x := it.Expr.(type) {
			case *sql.Col:
				name = x.Name
			case *sql.Func:
				name = x.Name
			case *sql.Cast:
				name = x.Type
			default:
				name = "?column?"
			}
		}
		res.cols = append(res.cols, name)
		res.kinds = append(res.kinds, kindOf(it.Expr, l))
	}

	agg := len(sel.GroupBy) > 0 || sel.Having != nil && hasAggregate(sel.Having)
	for _, it := range items {
		if hasAggregate(it.Expr) {
			agg = true
		}
	}
	for _, o := range order {
		if hasAggregate(o.Expr) {
			agg = true
		}
	}

	type out struct{ vals, keys []Value }
	var outs []out
	emit := func(e *env) error {
		var o out
		for _, it := range items {
			v, err := e.eval(it.Expr)
			if err != nil {
				return err
			}
			o.vals = append(o.vals, v)
		}
		for _, ob := range order {
			if pos := outputPos(ob.Expr, res.cols); pos >= 0 {
				o.keys = append(o.keys, o.vals[pos])
				continue
			}
			v, err := e.eval(ob.Expr)
			if err != nil {
				return err
			}
			o.keys = append(o.keys, v)
		}
		outs = append(outs, o)
		return nil
	}
	if agg {
		var order2 []string
		groups := map[string][][]Value{}
		e := &env{l: l, outer: outer, s: s, params: s.params}
		for _, r := range rows {
			e.row = r
			var kv []Value
			for _, g := range sel.GroupBy {
				if pos := outputPos(g, res.cols); pos >= 0 {
					// GROUP BY prefers an input column over an output alias of the same name
					if c, ok := g.(*sql.Col); !ok || len(l.byName[c.Name]) == 0 {
						g = items[pos].Expr
					}
				}
				v, err := e.eval(g)
				if err != nil {
					return nil, err
				}
				kv = append(kv, v)
			}
			k := key(kv)
			if _, ok := groups[k]; !ok {
				order2 = append(order2, k)
			}
			groups[k] = append(groups[k], r)
		}
		if len(sel.GroupBy) == 0 && len(rows) == 0 {
			order2 = append(order2, "")
			groups[""] = [][]Value{}
		}
		if sel.Having != nil && neverTrue(sel.Having) {
			order2 = nil
		}
		for _, k := range order2 {
			g := groups[k]
			e := &env{l: l, outer: outer, s: s, params: s.params, group: g}
			if len(g) > 0 {
				e.row = g[0]
			}
			if sel.Having != nil {
				v, err := e.eval(sel.Having)
				if err != nil {
					return nil, err
				}
				b, null, err := truth(v)
				if err != nil {
					return nil, err
				}
				if !b || null {
					continue
				}
			}
			if err := emit(e); err != nil {
				return nil, err
			}
		}
	} else {
		e := &env{l: l, outer: outer, s: s, params: s.params}
		for _, r := range rows {
			e.row = r
			if err := emit(e); err != nil {
				return nil, err
			}
		}
	}

	if len(order) > 0 {
		vals := make([][]Value, len(outs))
		keys := make([][]Value, len(outs))
		for i, o := range outs {
			vals[i], keys[i] = o.vals, o.keys
		}
		sortRows(vals, keys, order)
		res.rows = vals
	} else {
		for _, o := range outs {
			res.rows = append(res.rows, o.vals)
		}
	}
	if sel.Distinct {
		res.rows = distinct(res.rows)
	}
	for i, k := range res.kinds {
		if k != "" {
			continue
		}
		res.kinds[i] = "text"
		for _, r := range res.rows {
			if r[i] != nil {
				res.kinds[i] = typeOfValue(r[i])
				break
			}
		}
	}
	if final {
		return s.limit(sel, outer, res)
	}
	return res, nil
}

func kindOf(x sql.Expr, l *layout) string {
	switch x := x.(type) {
	case *sql.Col:
		for _, i := range l.byName[x.Name] {
			if x.Table == "" || l.cols[i].table == x.Table {
				return l.cols[i].kind
			}
		}
	case *sql.Lit:
		return typeOfValue(x.V)
	case *sql.Cast:
		return kind(x.Type)
	case *sql.Func:
		switch x.Name {
		case "count", "length", "char_length":
			return "int"
		case "avg", "total", "random", "sqrt":
			return "float"
		case "sum", "min", "max", "abs", "coalesce", "round", "floor", "ceil":
			if len(x.Args) > 0 {
				return kindOf(x.Args[0], l)
			}
		case "upper", "lower", "concat", "substr", "substring", "string_agg", "version", "current_database", "current_schema", "current_user":
			return "text"
		}
	case *sql.Binary:
		switch x.Op {
		case "+", "-", "*", "/", "%":
			a, b := kindOf(x.L, l), kindOf(x.R, l)
			if a == "int" && b == "int" {
				return "int"
			}
			if a == "" || b == "" {
				return ""
			}
			return "float"
		case "||":
			return "text"
		}
		return "bool"
	case *sql.Unary:
		if x.Op == "not" {
			return "bool"
		}
		return kindOf(x.X, l)
	case *sql.IsNull, *sql.Between, *sql.In, *sql.Exists, *sql.Like:
		return "bool"
	case *sql.Case:
		for _, w := range x.Whens {
			if k := kindOf(w.Then, l); k != "" {
				return k
			}
		}
		return kindOf(x.Else, l)
	}
	return ""
}
