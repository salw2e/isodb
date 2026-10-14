package sql

import (
	"fmt"
	"strconv"
	"strings"
)

type tok struct {
	kind byte // i ident, q quoted ident, n number, s string, p param, o operator, e end
	s    string
	pos  int
}

type Error struct{ Msg string }

func (e *Error) Error() string { return e.Msg }

func lex(src string) ([]tok, error) {
	var out []tok
	i := 0
	for i < len(src) {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '-' && i+1 < len(src) && src[i+1] == '-':
			for i < len(src) && src[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(src) && src[i+1] == '*':
			end := strings.Index(src[i+2:], "*/")
			if end < 0 {
				return nil, &Error{"unterminated comment"}
			}
			i += end + 4
		case isLetter(c) || c == '_':
			j := i
			for j < len(src) && (isLetter(src[j]) || isDigit(src[j]) || src[j] == '_' || src[j] == '$') {
				j++
			}
			w := src[i:j]
			if (w == "E" || w == "e") && j < len(src) && src[j] == '\'' {
				s, n, err := str(src[j:], true)
				if err != nil {
					return nil, err
				}
				out = append(out, tok{'s', s, i})
				i = j + n
				continue
			}
			out = append(out, tok{'i', strings.ToLower(w), i})
			i = j
		case isDigit(c) || c == '.' && i+1 < len(src) && isDigit(src[i+1]):
			j := i
			for j < len(src) && (isDigit(src[j]) || src[j] == '.') {
				j++
			}
			if j < len(src) && (src[j] == 'e' || src[j] == 'E') {
				k := j + 1
				if k < len(src) && (src[k] == '+' || src[k] == '-') {
					k++
				}
				if k < len(src) && isDigit(src[k]) {
					j = k
					for j < len(src) && isDigit(src[j]) {
						j++
					}
				}
			}
			out = append(out, tok{'n', src[i:j], i})
			i = j
		case c == '\'':
			s, n, err := str(src[i:], false)
			if err != nil {
				return nil, err
			}
			out = append(out, tok{'s', s, i})
			i += n
		case c == '"':
			j := i + 1
			var b strings.Builder
			for {
				if j >= len(src) {
					return nil, &Error{"unterminated quoted identifier"}
				}
				if src[j] == '"' {
					if j+1 < len(src) && src[j+1] == '"' {
						b.WriteByte('"')
						j += 2
						continue
					}
					break
				}
				b.WriteByte(src[j])
				j++
			}
			out = append(out, tok{'q', b.String(), i})
			i = j + 1
		case c == '$' && i+1 < len(src) && isDigit(src[i+1]):
			j := i + 1
			for j < len(src) && isDigit(src[j]) {
				j++
			}
			out = append(out, tok{'p', src[i+1 : j], i})
			i = j
		default:
			two := ""
			if i+1 < len(src) {
				two = src[i : i+2]
			}
			switch two {
			case "<=", ">=", "<>", "!=", "||", "::":
				out = append(out, tok{'o', two, i})
				i += 2
				continue
			}
			if strings.IndexByte("=<>+-*/%(),.;[]", c) < 0 {
				return nil, &Error{fmt.Sprintf("syntax error at or near \"%c\"", c)}
			}
			out = append(out, tok{'o', string(c), i})
			i++
		}
	}
	return append(out, tok{'e', "", len(src)}), nil
}

func str(src string, esc bool) (string, int, error) {
	var b strings.Builder
	j := 1
	for {
		if j >= len(src) {
			return "", 0, &Error{"unterminated quoted string"}
		}
		c := src[j]
		if c == '\'' {
			if j+1 < len(src) && src[j+1] == '\'' {
				b.WriteByte('\'')
				j += 2
				continue
			}
			return b.String(), j + 1, nil
		}
		if esc && c == '\\' && j+1 < len(src) {
			j++
			switch src[j] {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case 'r':
				b.WriteByte('\r')
			default:
				b.WriteByte(src[j])
			}
			j++
			continue
		}
		b.WriteByte(c)
		j++
	}
}

func isLetter(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80 }
func isDigit(c byte) bool  { return c >= '0' && c <= '9' }

type parser struct {
	t      []tok
	p      int
	params int
}

// Parse splits src into statements and parses each one. It also returns each statement's text.
func Parse(src string) ([]Stmt, []string, int, error) {
	t, err := lex(src)
	if err != nil {
		return nil, nil, 0, err
	}
	ps := &parser{t: t}
	var out []Stmt
	var texts []string
	for {
		for ps.op(";") {
		}
		if ps.peek().kind == 'e' {
			break
		}
		from := ps.peek().pos
		s, err := ps.stmt()
		if err != nil {
			return nil, nil, 0, err
		}
		out = append(out, s)
		texts = append(texts, src[from:ps.peek().pos])
		if !ps.op(";") && ps.peek().kind != 'e' {
			return nil, nil, 0, ps.fail()
		}
	}
	return out, texts, ps.params, nil
}

func (p *parser) peek() tok    { return p.t[p.p] }
func (p *parser) peekN(n int) tok {
	if p.p+n < len(p.t) {
		return p.t[p.p+n]
	}
	return tok{'e', "", 0}
}

func (p *parser) fail() error {
	t := p.peek()
	if t.kind == 'e' {
		return &Error{"syntax error at end of input"}
	}
	return &Error{fmt.Sprintf("syntax error at or near \"%s\"", t.s)}
}

func (p *parser) kw(words ...string) bool {
	for i, w := range words {
		t := p.peekN(i)
		if t.kind != 'i' || t.s != w {
			return false
		}
	}
	p.p += len(words)
	return true
}

func (p *parser) isKw(w string) bool { t := p.peek(); return t.kind == 'i' && t.s == w }

func (p *parser) op(o string) bool {
	t := p.peek()
	if t.kind == 'o' && t.s == o {
		p.p++
		return true
	}
	return false
}

func (p *parser) expectKw(w string) error {
	if !p.kw(w) {
		return p.fail()
	}
	return nil
}

func (p *parser) expectOp(o string) error {
	if !p.op(o) {
		return p.fail()
	}
	return nil
}

var reserved = map[string]bool{
	"select": true, "from": true, "where": true, "group": true, "order": true, "by": true, "having": true,
	"limit": true, "offset": true, "union": true, "intersect": true, "except": true, "and": true, "or": true,
	"not": true, "on": true, "join": true, "inner": true, "left": true, "right": true, "full": true, "cross": true,
	"as": true, "into": true, "values": true, "set": true, "returning": true, "is": true, "in": true,
	"between": true, "like": true, "ilike": true, "case": true, "when": true, "then": true, "else": true, "end": true,
	"null": true, "true": true, "false": true, "distinct": true, "all": true, "outer": true, "for": true,
	"asc": true, "desc": true, "exists": true, "using": true, "natural": true, "default": true,
}

func (p *parser) ident() (string, error) {
	t := p.peek()
	if t.kind == 'q' || t.kind == 'i' && !reserved[t.s] {
		p.p++
		return t.s, nil
	}
	return "", p.fail()
}

// identifier that may also be a reserved word (after a dot, in column lists)
func (p *parser) name() (string, error) {
	t := p.peek()
	if t.kind == 'q' || t.kind == 'i' {
		p.p++
		return t.s, nil
	}
	return "", p.fail()
}

func (p *parser) qualified() (string, error) {
	n, err := p.ident()
	if err != nil {
		return "", err
	}
	for p.op(".") {
		if n, err = p.name(); err != nil {
			return "", err
		}
	}
	return n, nil
}

func (p *parser) stmt() (Stmt, error) {
	switch {
	case p.isKw("select") || p.peek().kind == 'o' && p.peek().s == "(" || p.isKw("values"):
		return p.selectStmt()
	case p.kw("insert"):
		return p.insert()
	case p.kw("update"):
		return p.update()
	case p.kw("delete"):
		return p.del()
	case p.kw("create"):
		unique := p.kw("unique")
		if p.kw("index") {
			return p.createIndex(unique)
		}
		if unique {
			return nil, p.fail()
		}
		p.kw("temporary")
		p.kw("temp")
		if p.kw("view") {
			v := &CreateView{IfNotExists: p.kw("if", "not", "exists")}
			n, err := p.qualified()
			if err != nil {
				return nil, err
			}
			v.Name = n
			if p.peek().kind == 'o' && p.peek().s == "(" {
				if err := p.skipParens(); err != nil {
					return nil, err
				}
			}
			if err := p.expectKw("as"); err != nil {
				return nil, err
			}
			v.Sel, err = p.selectStmt()
			return v, err
		}
		if p.kw("table") {
			return p.createTable()
		}
		return nil, p.fail()
	case p.kw("drop"):
		if p.kw("table") {
			d := &DropTable{IfExists: p.kw("if", "exists")}
			for {
				n, err := p.qualified()
				if err != nil {
					return nil, err
				}
				d.Names = append(d.Names, n)
				if !p.op(",") {
					break
				}
			}
			p.kw("cascade")
			return d, nil
		}
		if p.kw("view") {
			d := &DropView{IfExists: p.kw("if", "exists")}
			for {
				n, err := p.qualified()
				if err != nil {
					return nil, err
				}
				d.Names = append(d.Names, n)
				if !p.op(",") {
					break
				}
			}
			return d, nil
		}
		if p.kw("index") {
			d := &DropIndex{IfExists: p.kw("if", "exists")}
			n, err := p.qualified()
			d.Name = n
			return d, err
		}
		return nil, p.fail()
	case p.kw("begin") || p.kw("start", "transaction"):
		p.kw("transaction")
		p.kw("work")
		iso, err := p.isoOpt()
		return &Begin{Iso: iso}, err
	case p.kw("commit") || p.kw("end"):
		p.kw("transaction")
		p.kw("work")
		return &Commit{}, nil
	case p.kw("rollback") || p.kw("abort"):
		p.kw("transaction")
		p.kw("work")
		return &Rollback{}, nil
	case p.kw("set"):
		if p.kw("transaction") || p.kw("session", "characteristics", "as", "transaction") {
			iso, err := p.isoOpt()
			return &SetTx{Iso: iso}, err
		}
		p.kw("session")
		p.kw("local")
		n, err := p.qualified()
		if err != nil {
			return nil, err
		}
		if !p.kw("to") && !p.op("=") {
			return nil, p.fail()
		}
		var vals []string
		for p.peek().kind != 'e' && !(p.peek().kind == 'o' && p.peek().s == ";") {
			if t := p.peek(); t.kind != 'o' || t.s != "," {
				vals = append(vals, t.s)
			}
			p.p++
		}
		return &SetVar{Name: n, Value: strings.Join(vals, ", ")}, nil
	case p.kw("show"):
		var parts []string
		for p.peek().kind == 'i' || p.peek().kind == 'q' {
			parts = append(parts, p.peek().s)
			p.p++
			if !p.op(".") {
				break
			}
		}
		return &Show{Name: strings.Join(parts, " ")}, nil
	}
	return nil, p.fail()
}

func (p *parser) isoOpt() (string, error) {
	iso := ""
	for {
		switch {
		case p.kw("isolation", "level"):
			switch {
			case p.kw("serializable"):
				iso = "serializable"
			case p.kw("repeatable", "read"):
				iso = "repeatable read"
			case p.kw("read", "committed"):
				iso = "read committed"
			case p.kw("read", "uncommitted"):
				iso = "read committed"
			default:
				return "", p.fail()
			}
		case p.kw("read", "write"), p.kw("read", "only"), p.kw("deferrable"), p.kw("not", "deferrable"):
		default:
			return iso, nil
		}
		p.op(",")
	}
}

func (p *parser) createTable() (Stmt, error) {
	ct := &CreateTable{IfNotExists: p.kw("if", "not", "exists")}
	var err error
	if ct.Name, err = p.qualified(); err != nil {
		return nil, err
	}
	if err = p.expectOp("("); err != nil {
		return nil, err
	}
	for {
		switch {
		case p.kw("primary", "key"):
			cols, err := p.nameList()
			if err != nil {
				return nil, err
			}
			ct.PK = cols
		case p.kw("unique"):
			if _, err := p.nameList(); err != nil {
				return nil, err
			}
		case p.kw("constraint"):
			if _, err := p.ident(); err != nil {
				return nil, err
			}
			continue
		case p.kw("foreign", "key"):
			if err := p.skipParens(); err != nil {
				return nil, err
			}
			p.references()
		case p.kw("check"):
			if err := p.skipParens(); err != nil {
				return nil, err
			}
		default:
			c, err := p.colDef()
			if err != nil {
				return nil, err
			}
			ct.Cols = append(ct.Cols, c)
		}
		if !p.op(",") {
			break
		}
	}
	return ct, p.expectOp(")")
}

func (p *parser) skipParens() error {
	if err := p.expectOp("("); err != nil {
		return err
	}
	for depth := 1; depth > 0; p.p++ {
		t := p.peek()
		if t.kind == 'e' {
			return p.fail()
		}
		if t.kind == 'o' && t.s == "(" {
			depth++
		}
		if t.kind == 'o' && t.s == ")" {
			depth--
		}
	}
	return nil
}

func (p *parser) references() {
	if p.kw("references") {
		p.qualified()
		if p.peek().kind == 'o' && p.peek().s == "(" {
			p.skipParens()
		}
		for p.kw("on", "delete") || p.kw("on", "update") {
			if !p.kw("cascade") && !p.kw("restrict") && !p.kw("set", "null") && !p.kw("set", "default") {
				p.kw("no", "action")
			}
		}
	}
}

func (p *parser) nameList() ([]string, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	var out []string
	for {
		n, err := p.name()
		if err != nil {
			return nil, err
		}
		p.kw("asc")
		p.kw("desc")
		out = append(out, n)
		if !p.op(",") {
			break
		}
	}
	return out, p.expectOp(")")
}

func (p *parser) typeName() (string, error) {
	t := p.peek()
	if t.kind != 'i' && t.kind != 'q' {
		return "", p.fail()
	}
	p.p++
	n := t.s
	switch n {
	case "double":
		p.kw("precision")
		n = "float8"
	case "character":
		if p.kw("varying") {
			n = "varchar"
		} else {
			n = "char"
		}
	case "timestamp", "time":
		if p.kw("with", "time", "zone") {
			n += "tz"
		}
		p.kw("without", "time", "zone")
	}
	if p.op("(") {
		for !p.op(")") {
			if p.peek().kind == 'e' {
				return "", p.fail()
			}
			p.p++
		}
	}
	for p.op("[") {
		p.op("]")
		n += "[]"
	}
	return n, nil
}

func (p *parser) colDef() (ColDef, error) {
	var c ColDef
	var err error
	if c.Name, err = p.name(); err != nil {
		return c, err
	}
	if c.Type, err = p.typeName(); err != nil {
		return c, err
	}
	switch c.Type {
	case "serial", "serial4", "bigserial", "serial8", "smallserial":
		c.Serial = true
		c.Type = "integer"
	}
	for {
		switch {
		case p.kw("primary", "key"):
			c.PK, c.NotNull = true, true
		case p.kw("not", "null"):
			c.NotNull = true
		case p.kw("null"):
		case p.kw("unique"):
			c.Unique = true
		case p.kw("default"):
			if c.Default, err = p.expr(); err != nil {
				return c, err
			}
		case p.kw("generated"):
			p.kw("always")
			p.kw("by", "default")
			if err := p.expectKw("as"); err != nil {
				return c, err
			}
			if err := p.expectKw("identity"); err != nil {
				return c, err
			}
			c.Serial = true
		case p.kw("constraint"):
			if _, err := p.ident(); err != nil {
				return c, err
			}
		case p.kw("check"):
			if err := p.skipParens(); err != nil {
				return c, err
			}
		case p.isKw("references"):
			p.references()
		case p.kw("collate"):
			p.name()
		default:
			return c, nil
		}
	}
}

func (p *parser) createIndex(unique bool) (Stmt, error) {
	ci := &CreateIndex{Unique: unique}
	p.kw("concurrently")
	ci.IfNotExists = p.kw("if", "not", "exists")
	if !p.isKw("on") {
		n, err := p.qualified()
		if err != nil {
			return nil, err
		}
		ci.Name = n
	}
	if err := p.expectKw("on"); err != nil {
		return nil, err
	}
	var err error
	if ci.Table, err = p.qualified(); err != nil {
		return nil, err
	}
	if p.kw("using") {
		p.ident()
	}
	ci.Cols, err = p.nameList()
	return ci, err
}

func (p *parser) insert() (Stmt, error) {
	if err := p.expectKw("into"); err != nil {
		return nil, err
	}
	ins := &Insert{}
	var err error
	if ins.Table, err = p.qualified(); err != nil {
		return nil, err
	}
	if p.peek().kind == 'o' && p.peek().s == "(" && !(p.peekN(1).kind == 'i' && (p.peekN(1).s == "select" || p.peekN(1).s == "values")) {
		if ins.Cols, err = p.nameList(); err != nil {
			return nil, err
		}
	}
	if p.kw("default", "values") {
		ins.Rows = [][]Expr{{}}
	} else if p.kw("values") {
		for {
			if err := p.expectOp("("); err != nil {
				return nil, err
			}
			var row []Expr
			for {
				var e Expr
				if p.kw("default") {
					e = &Func{Name: "$default"}
				} else if e, err = p.expr(); err != nil {
					return nil, err
				}
				row = append(row, e)
				if !p.op(",") {
					break
				}
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
			ins.Rows = append(ins.Rows, row)
			if !p.op(",") {
				break
			}
		}
	} else {
		s, err := p.selectStmt()
		if err != nil {
			return nil, err
		}
		ins.Select = s
	}
	if p.kw("on", "conflict") {
		oc := &OnConflict{}
		if p.peek().kind == 'o' && p.peek().s == "(" {
			if oc.Cols, err = p.nameList(); err != nil {
				return nil, err
			}
		}
		if err := p.expectKw("do"); err != nil {
			return nil, err
		}
		if p.kw("nothing") {
			oc.Nothing = true
		} else {
			if err := p.expectKw("update"); err != nil {
				return nil, err
			}
			if err := p.expectKw("set"); err != nil {
				return nil, err
			}
			if oc.Sets, err = p.sets(); err != nil {
				return nil, err
			}
		}
		ins.Conflict = oc
	}
	ins.Returning, err = p.returning()
	return ins, err
}

func (p *parser) returning() ([]SelItem, error) {
	if !p.kw("returning") {
		return nil, nil
	}
	return p.selItems()
}

func (p *parser) sets() ([]Set, error) {
	var out []Set
	for {
		n, err := p.name()
		if err != nil {
			return nil, err
		}
		if p.op(".") {
			if n, err = p.name(); err != nil {
				return nil, err
			}
		}
		if err := p.expectOp("="); err != nil {
			return nil, err
		}
		var e Expr
		if p.kw("default") {
			e = &Func{Name: "$default"}
		} else if e, err = p.expr(); err != nil {
			return nil, err
		}
		out = append(out, Set{n, e})
		if !p.op(",") {
			return out, nil
		}
	}
}

func (p *parser) alias() string {
	if p.kw("as") {
		n, _ := p.name()
		return n
	}
	if t := p.peek(); t.kind == 'q' || t.kind == 'i' && !reserved[t.s] && t.s != "set" {
		p.p++
		return t.s
	}
	return ""
}

func (p *parser) update() (Stmt, error) {
	u := &Update{}
	var err error
	if u.Table, err = p.qualified(); err != nil {
		return nil, err
	}
	u.Alias = p.alias()
	if err := p.expectKw("set"); err != nil {
		return nil, err
	}
	if u.Sets, err = p.sets(); err != nil {
		return nil, err
	}
	if p.kw("where") {
		if u.Where, err = p.expr(); err != nil {
			return nil, err
		}
	}
	u.Returning, err = p.returning()
	return u, err
}

func (p *parser) del() (Stmt, error) {
	if err := p.expectKw("from"); err != nil {
		return nil, err
	}
	d := &Delete{}
	var err error
	if d.Table, err = p.qualified(); err != nil {
		return nil, err
	}
	d.Alias = p.alias()
	if p.kw("where") {
		if d.Where, err = p.expr(); err != nil {
			return nil, err
		}
	}
	d.Returning, err = p.returning()
	return d, err
}

func (p *parser) selectStmt() (*Select, error) {
	s, err := p.selectCore()
	if err != nil {
		return nil, err
	}
	for {
		var op string
		switch {
		case p.kw("union"):
			op = "union"
		case p.kw("intersect"):
			op = "intersect"
		case p.kw("except"):
			op = "except"
		default:
			goto tail
		}
		all := p.kw("all")
		p.kw("distinct")
		r, err := p.selectCore()
		if err != nil {
			return nil, err
		}
		last := s
		for last.Right != nil {
			last = last.Right
		}
		last.Op, last.All, last.Right = op, all, r
	}
tail:
	if p.kw("order", "by") {
		for {
			e, err := p.expr()
			if err != nil {
				return nil, err
			}
			o := Order{Expr: e}
			if p.kw("desc") {
				o.Desc = true
			} else {
				p.kw("asc")
			}
			if p.kw("nulls", "first") || p.kw("nulls", "last") {
			}
			s.OrderBy = append(s.OrderBy, o)
			if !p.op(",") {
				break
			}
		}
	}
	for {
		if p.kw("limit") {
			if p.kw("all") {
				continue
			}
			if s.Limit, err = p.expr(); err != nil {
				return nil, err
			}
		} else if p.kw("offset") {
			if s.Offset, err = p.expr(); err != nil {
				return nil, err
			}
			p.kw("rows")
			p.kw("row")
		} else if p.kw("for", "update") || p.kw("for", "share") || p.kw("for", "no", "key", "update") || p.kw("for", "key", "share") {
			s.ForUpdate = true
			p.kw("nowait")
		} else {
			return s, nil
		}
	}
}

func (p *parser) selectCore() (*Select, error) {
	if p.op("(") {
		s, err := p.selectStmt()
		if err != nil {
			return nil, err
		}
		return s, p.expectOp(")")
	}
	if p.kw("values") {
		// VALUES (..),(..) as a select of literal rows
		s := &Select{}
		var rows []*Select
		for {
			if err := p.expectOp("("); err != nil {
				return nil, err
			}
			r := &Select{}
			for i := 1; ; i++ {
				e, err := p.expr()
				if err != nil {
					return nil, err
				}
				r.Items = append(r.Items, SelItem{Expr: e, Alias: "column" + strconv.Itoa(i)})
				if !p.op(",") {
					break
				}
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
			rows = append(rows, r)
			if !p.op(",") {
				break
			}
		}
		*s = *rows[0]
		last := s
		for _, r := range rows[1:] {
			last.Op, last.All, last.Right = "union", true, r
			last = r
		}
		return s, nil
	}
	if err := p.expectKw("select"); err != nil {
		return nil, err
	}
	s := &Select{}
	if p.kw("distinct") {
		s.Distinct = true
	} else {
		p.kw("all")
	}
	var err error
	if s.Items, err = p.selItems(); err != nil {
		return nil, err
	}
	if p.kw("from") {
		if s.From, err = p.fromList(); err != nil {
			return nil, err
		}
	}
	if p.kw("where") {
		if s.Where, err = p.expr(); err != nil {
			return nil, err
		}
	}
	if p.kw("group", "by") {
		for {
			e, err := p.expr()
			if err != nil {
				return nil, err
			}
			s.GroupBy = append(s.GroupBy, e)
			if !p.op(",") {
				break
			}
		}
	}
	if p.kw("having") {
		if s.Having, err = p.expr(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (p *parser) selItems() ([]SelItem, error) {
	var out []SelItem
	for {
		if p.op("*") {
			out = append(out, SelItem{Star: true})
		} else if t := p.peek(); (t.kind == 'i' || t.kind == 'q') && p.peekN(1).kind == 'o' && p.peekN(1).s == "." && p.peekN(2).kind == 'o' && p.peekN(2).s == "*" {
			p.p += 3
			out = append(out, SelItem{Star: true, Table: t.s})
		} else {
			e, err := p.expr()
			if err != nil {
				return nil, err
			}
			out = append(out, SelItem{Expr: e, Alias: p.alias()})
		}
		if !p.op(",") {
			return out, nil
		}
	}
}

func (p *parser) fromList() ([]TableRef, error) {
	var out []TableRef
	t, err := p.tableRef()
	if err != nil {
		return nil, err
	}
	out = append(out, t)
	for {
		join := ""
		switch {
		case p.op(","):
			join = ","
		case p.kw("cross", "join"):
			join = "cross"
		case p.kw("join"), p.kw("inner", "join"):
			join = "inner"
		case p.kw("left", "join"), p.kw("left", "outer", "join"):
			join = "left"
		default:
			return out, nil
		}
		t, err := p.tableRef()
		if err != nil {
			return nil, err
		}
		t.Join = join
		if join == "inner" || join == "left" {
			if err := p.expectKw("on"); err != nil {
				return nil, err
			}
			if t.On, err = p.expr(); err != nil {
				return nil, err
			}
		}
		out = append(out, t)
	}
}

func (p *parser) tableRef() (TableRef, error) {
	var t TableRef
	if p.op("(") {
		s, err := p.selectStmt()
		if err != nil {
			return t, err
		}
		if err := p.expectOp(")"); err != nil {
			return t, err
		}
		t.Sub = s
		t.Alias = p.alias()
		return t, nil
	}
	var err error
	if t.Name, err = p.qualified(); err != nil {
		return t, err
	}
	t.Alias = p.alias()
	return t, nil
}

// expressions, lowest precedence first

func (p *parser) expr() (Expr, error) { return p.or() }

func (p *parser) or() (Expr, error) {
	l, err := p.and()
	for err == nil && p.kw("or") {
		var r Expr
		if r, err = p.and(); err == nil {
			l = &Binary{"or", l, r}
		}
	}
	return l, err
}

func (p *parser) and() (Expr, error) {
	l, err := p.not()
	for err == nil && p.kw("and") {
		var r Expr
		if r, err = p.not(); err == nil {
			l = &Binary{"and", l, r}
		}
	}
	return l, err
}

func (p *parser) not() (Expr, error) {
	if p.kw("not") {
		x, err := p.not()
		return &Unary{"not", x}, err
	}
	return p.is()
}

func (p *parser) is() (Expr, error) {
	l, err := p.cmp()
	for err == nil {
		switch {
		case p.kw("is", "not", "null"):
			l = &IsNull{l, true}
		case p.kw("is", "null"), p.kw("isnull"):
			l = &IsNull{l, false}
		case p.kw("notnull"):
			l = &IsNull{l, true}
		case p.kw("is", "not", "distinct", "from"):
			r, e := p.cmp()
			l, err = &Binary{"is not distinct", l, r}, e
		case p.kw("is", "distinct", "from"):
			r, e := p.cmp()
			l, err = &Binary{"is distinct", l, r}, e
		case p.kw("is", "true"):
			l = &Binary{"is not distinct", l, &Lit{true}}
		case p.kw("is", "false"):
			l = &Binary{"is not distinct", l, &Lit{false}}
		case p.kw("is", "not", "true"):
			l = &Binary{"is distinct", l, &Lit{true}}
		case p.kw("is", "not", "false"):
			l = &Binary{"is distinct", l, &Lit{false}}
		default:
			return l, nil
		}
	}
	return l, err
}

func (p *parser) cmp() (Expr, error) {
	l, err := p.pred()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind != 'o' {
			return l, nil
		}
		op := t.s
		switch op {
		case "=", "<>", "!=", "<", "<=", ">", ">=":
		default:
			return l, nil
		}
		p.p++
		if op == "!=" {
			op = "<>"
		}
		if p.kw("any") || p.kw("some") || p.kw("all") {
			all := p.t[p.p-1].s == "all"
			if err := p.expectOp("("); err != nil {
				return nil, err
			}
			s, err := p.selectStmt()
			if err != nil {
				return nil, err
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
			fn := "$any"
			if all {
				fn = "$all"
			}
			l = &Func{Name: fn, Args: []Expr{&Lit{op}, l, &Subquery{s}}}
			continue
		}
		r, err := p.pred()
		if err != nil {
			return nil, err
		}
		l = &Binary{op, l, r}
	}
}

func (p *parser) pred() (Expr, error) {
	l, err := p.concat()
	if err != nil {
		return nil, err
	}
	for {
		not := false
		save := p.p
		if p.kw("not") {
			not = true
		}
		switch {
		case p.kw("between"):
			p.kw("symmetric")
			lo, err := p.concat()
			if err != nil {
				return nil, err
			}
			if err := p.expectKw("and"); err != nil {
				return nil, err
			}
			hi, err := p.concat()
			if err != nil {
				return nil, err
			}
			l = &Between{l, lo, hi, not}
		case p.kw("in"):
			if err := p.expectOp("("); err != nil {
				return nil, err
			}
			in := &In{X: l, Not: not}
			if p.isKw("select") || p.isKw("values") {
				if in.Sub, err = p.selectStmt(); err != nil {
					return nil, err
				}
			} else if !(p.peek().kind == 'o' && p.peek().s == ")") {
				for {
					e, err := p.expr()
					if err != nil {
						return nil, err
					}
					in.List = append(in.List, e)
					if !p.op(",") {
						break
					}
				}
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
			l = in
		case p.kw("like"), p.kw("ilike"):
			il := p.t[p.p-1].s == "ilike"
			r, err := p.concat()
			if err != nil {
				return nil, err
			}
			l = &Like{l, r, not, il}
		default:
			p.p = save
			return l, nil
		}
	}
}

func (p *parser) concat() (Expr, error) {
	l, err := p.add()
	for err == nil && p.op("||") {
		var r Expr
		if r, err = p.add(); err == nil {
			l = &Binary{"||", l, r}
		}
	}
	return l, err
}

func (p *parser) add() (Expr, error) {
	l, err := p.mul()
	for err == nil {
		t := p.peek()
		if t.kind != 'o' || t.s != "+" && t.s != "-" {
			return l, nil
		}
		p.p++
		var r Expr
		if r, err = p.mul(); err == nil {
			l = &Binary{t.s, l, r}
		}
	}
	return l, err
}

func (p *parser) mul() (Expr, error) {
	l, err := p.unary()
	for err == nil {
		t := p.peek()
		if t.kind != 'o' || t.s != "*" && t.s != "/" && t.s != "%" {
			return l, nil
		}
		p.p++
		var r Expr
		if r, err = p.unary(); err == nil {
			l = &Binary{t.s, l, r}
		}
	}
	return l, err
}

func (p *parser) unary() (Expr, error) {
	if p.op("-") {
		x, err := p.unary()
		if l, ok := x.(*Lit); ok {
			switch v := l.V.(type) {
			case int64:
				return &Lit{-v}, err
			case float64:
				return &Lit{-v}, err
			}
		}
		return &Unary{"-", x}, err
	}
	if p.op("+") {
		return p.unary()
	}
	return p.postfix()
}

func (p *parser) postfix() (Expr, error) {
	x, err := p.primary()
	for err == nil && p.op("::") {
		var t string
		if t, err = p.typeName(); err == nil {
			x = &Cast{x, t}
		}
	}
	return x, err
}

func (p *parser) primary() (Expr, error) {
	t := p.peek()
	switch t.kind {
	case 'n':
		p.p++
		if !strings.ContainsAny(t.s, ".eE") {
			if v, err := strconv.ParseInt(t.s, 10, 64); err == nil {
				return &Lit{v}, nil
			}
		}
		v, err := strconv.ParseFloat(t.s, 64)
		if err != nil {
			return nil, &Error{"invalid number " + t.s}
		}
		return &Lit{v}, nil
	case 's':
		p.p++
		return &Lit{t.s}, nil
	case 'p':
		p.p++
		n, _ := strconv.Atoi(t.s)
		if n > p.params {
			p.params = n
		}
		return &Param{n}, nil
	case 'o':
		if t.s == "(" {
			p.p++
			if p.isKw("select") || p.isKw("values") {
				s, err := p.selectStmt()
				if err != nil {
					return nil, err
				}
				return &Subquery{s}, p.expectOp(")")
			}
			e, err := p.expr()
			if err != nil {
				return nil, err
			}
			return e, p.expectOp(")")
		}
		return nil, p.fail()
	case 'q':
		return p.colRef()
	}
	switch {
	case p.kw("null"):
		return &Lit{nil}, nil
	case p.kw("true"):
		return &Lit{true}, nil
	case p.kw("false"):
		return &Lit{false}, nil
	case p.kw("case"):
		return p.caseExpr()
	case p.kw("exists"):
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		s, err := p.selectStmt()
		if err != nil {
			return nil, err
		}
		return &Exists{Sub: s}, p.expectOp(")")
	case p.kw("not", "exists"):
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		s, err := p.selectStmt()
		if err != nil {
			return nil, err
		}
		return &Exists{Sub: s, Not: true}, p.expectOp(")")
	case p.kw("cast"):
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		x, err := p.expr()
		if err != nil {
			return nil, err
		}
		if err := p.expectKw("as"); err != nil {
			return nil, err
		}
		ty, err := p.typeName()
		if err != nil {
			return nil, err
		}
		return &Cast{x, ty}, p.expectOp(")")
	}
	if t.kind == 'i' {
		// typed literal: integer '5', date '2020-01-01'
		if p.peekN(1).kind == 's' && (t.s == "date" || t.s == "timestamp" || t.s == "integer" || t.s == "int" || t.s == "text" || t.s == "boolean") {
			p.p += 2
			return &Cast{&Lit{p.t[p.p-1].s}, t.s}, nil
		}
		if p.peekN(1).kind == 'o' && p.peekN(1).s == "(" {
			p.p += 2
			f := &Func{Name: t.s}
			if p.op("*") {
				f.Star = true
				return f, p.expectOp(")")
			}
			if p.op(")") {
				return f, nil
			}
			if p.kw("distinct") {
				f.Distinct = true
			} else {
				p.kw("all")
			}
			for {
				e, err := p.expr()
				if err != nil {
					return nil, err
				}
				f.Args = append(f.Args, e)
				if !p.op(",") {
					break
				}
			}
			return f, p.expectOp(")")
		}
		if reserved[t.s] {
			return nil, p.fail()
		}
		return p.colRef()
	}
	return nil, p.fail()
}

func (p *parser) colRef() (Expr, error) {
	n, err := p.name()
	if err != nil {
		return nil, err
	}
	c := &Col{Name: n}
	for p.op(".") {
		c.Table = c.Name
		if c.Name, err = p.name(); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (p *parser) caseExpr() (Expr, error) {
	c := &Case{}
	var err error
	if !p.isKw("when") {
		if c.Operand, err = p.expr(); err != nil {
			return nil, err
		}
	}
	for p.kw("when") {
		var w When
		if w.Cond, err = p.expr(); err != nil {
			return nil, err
		}
		if err := p.expectKw("then"); err != nil {
			return nil, err
		}
		if w.Then, err = p.expr(); err != nil {
			return nil, err
		}
		c.Whens = append(c.Whens, w)
	}
	if p.kw("else") {
		if c.Else, err = p.expr(); err != nil {
			return nil, err
		}
	}
	return c, p.expectKw("end")
}
