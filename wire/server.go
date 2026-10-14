package wire

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync/atomic"

	"isodb/db"
	"isodb/sql"
)

type prepared struct {
	stmt   sql.Stmt
	text   string
	params []int32
	cols   []string
	kinds  []string
	empty  bool
}

type portal struct {
	p       *prepared
	params  []db.Value
	formats []int16
	res     *db.Result
	pos     int
}

type conn struct {
	c       net.Conn
	r       *bufio.Reader
	w       *bufio.Writer
	s       *db.Session
	stmts   map[string]*prepared
	portals map[string]*portal
	failed  bool
}

var pids int32

func Serve(ln net.Listener, d *db.DB) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go func() {
			defer c.Close()
			cn := &conn{c: c, r: bufio.NewReader(c), w: bufio.NewWriter(c), stmts: map[string]*prepared{}, portals: map[string]*portal{}}
			if err := cn.serve(d); err != nil && !errors.Is(err, io.EOF) {
				fmt.Println("connection:", err)
			}
			if cn.s != nil {
				cn.s.Close()
			}
		}()
	}
}

func (c *conn) msg(t byte, body []byte) {
	c.w.WriteByte(t)
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(body)+4))
	c.w.Write(n[:])
	c.w.Write(body)
}

func str0(b []byte, s string) []byte { return append(append(b, s...), 0) }

func (c *conn) startup(d *db.DB) error {
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
			return err
		}
		n := int(binary.BigEndian.Uint32(hdr[:4]))
		code := binary.BigEndian.Uint32(hdr[4:])
		if n < 8 || n > 1<<16 {
			return fmt.Errorf("bad startup length %d", n)
		}
		body := make([]byte, n-8)
		if _, err := io.ReadFull(c.r, body); err != nil {
			return err
		}
		switch code {
		case 80877103, 80877104: // SSL or GSS encryption request: not supported, carry on in plain text
			c.w.WriteByte('N')
			c.w.Flush()
			continue
		case 80877102: // cancel request
			return io.EOF
		case 196608:
		default:
			return fmt.Errorf("unsupported protocol %d", code)
		}
		params := map[string]string{}
		parts := splitZero(body)
		for i := 0; i+1 < len(parts); i += 2 {
			params[parts[i]] = parts[i+1]
		}
		user := params["user"]
		c.s = d.NewSession(user)
		c.msg('R', []byte{0, 0, 0, 0})
		for _, kv := range [][2]string{
			{"server_version", "14.0"}, {"server_encoding", "UTF8"}, {"client_encoding", "UTF8"},
			{"DateStyle", "ISO, MDY"}, {"TimeZone", "UTC"}, {"integer_datetimes", "on"},
			{"standard_conforming_strings", "on"}, {"IntervalStyle", "postgres"}, {"is_superuser", "on"},
			{"session_authorization", user}, {"application_name", params["application_name"]},
		} {
			c.msg('S', str0(str0(nil, kv[0]), kv[1]))
		}
		var k [8]byte
		binary.BigEndian.PutUint32(k[:], uint32(atomic.AddInt32(&pids, 1)))
		binary.BigEndian.PutUint32(k[4:], 0x5eed)
		c.msg('K', k[:])
		c.ready()
		return c.w.Flush()
	}
}

func splitZero(b []byte) []string {
	var out []string
	start := 0
	for i, x := range b {
		if x == 0 {
			if i == start {
				break
			}
			out = append(out, string(b[start:i]))
			start = i + 1
		}
	}
	return out
}

func (c *conn) ready() { c.msg('Z', []byte{c.s.Status()}) }

func (c *conn) fail(err error) {
	code, text := "XX000", err.Error()
	var de *db.Error
	var se *sql.Error
	switch {
	case errors.As(err, &de):
		code = de.Code
	case errors.As(err, &se):
		code = "42601"
	}
	b := str0(append([]byte(nil), 'S'), "ERROR")
	b = str0(append(b, 'V'), "ERROR")
	b = str0(append(b, 'C'), code)
	b = str0(append(b, 'M'), text)
	c.msg('E', append(b, 0))
}

func (c *conn) serve(d *db.DB) error {
	if err := c.startup(d); err != nil {
		return err
	}
	for {
		t, err := c.r.ReadByte()
		if err != nil {
			return err
		}
		var nb [4]byte
		if _, err := io.ReadFull(c.r, nb[:]); err != nil {
			return err
		}
		n := int(binary.BigEndian.Uint32(nb[:])) - 4
		if n < 0 || n > 1<<30 {
			return fmt.Errorf("bad message length")
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(c.r, body); err != nil {
			return err
		}
		if c.failed && t != 'S' && t != 'X' {
			continue
		}
		switch t {
		case 'Q':
			c.simple(string(trimZero(body)))
			c.ready()
		case 'P':
			c.parse(body)
		case 'B':
			c.bind(body)
		case 'D':
			c.describe(body)
		case 'E':
			c.execute(body)
		case 'S':
			c.failed = false
			c.ready()
		case 'H':
		case 'C':
			name := string(trimZero(body[1:]))
			if body[0] == 'S' {
				delete(c.stmts, name)
			} else {
				delete(c.portals, name)
			}
			c.msg('3', nil)
		case 'X':
			c.w.Flush()
			return nil
		default:
			c.fail(fmt.Errorf("unsupported message %q", t))
		}
		if t == 'S' || t == 'Q' || t == 'H' || c.r.Buffered() == 0 {
			if err := c.w.Flush(); err != nil {
				return err
			}
		}
	}
}

func trimZero(b []byte) []byte {
	if len(b) > 0 && b[len(b)-1] == 0 {
		return b[:len(b)-1]
	}
	return b
}

func (c *conn) simple(q string) {
	stmts, texts, _, err := sql.Parse(q)
	if err != nil {
		c.fail(err)
		return
	}
	if len(stmts) == 0 {
		c.msg('I', nil)
		return
	}
	for i, st := range stmts {
		res, err := c.s.Run(st, texts[i], nil)
		if err != nil {
			c.fail(err)
			return
		}
		if res.Cols != nil {
			c.rowDesc(res.Cols, res.Kinds, nil)
			for _, r := range res.Rows {
				c.dataRow(r, res.Kinds, nil)
			}
		}
		c.msg('C', str0(nil, res.Tag))
	}
}

func (c *conn) rowDesc(cols, kinds []string, formats []int16) {
	b := binary.BigEndian.AppendUint16(nil, uint16(len(cols)))
	for i, name := range cols {
		b = str0(b, name)
		b = binary.BigEndian.AppendUint32(b, 0)
		b = binary.BigEndian.AppendUint16(b, 0)
		o, size := typeOID(kinds[i])
		b = binary.BigEndian.AppendUint32(b, uint32(o))
		b = binary.BigEndian.AppendUint16(b, uint16(size))
		b = binary.BigEndian.AppendUint32(b, 0xffffffff)
		b = binary.BigEndian.AppendUint16(b, uint16(format(formats, i)))
	}
	c.msg('T', b)
}

func typeOID(k string) (int32, int16) {
	switch k {
	case "int":
		return 20, 8
	case "float":
		return 701, 8
	case "bool":
		return 16, 1
	}
	return 25, -1
}

func format(f []int16, i int) int16 {
	switch len(f) {
	case 0:
		return 0
	case 1:
		return f[0]
	}
	if i < len(f) {
		return f[i]
	}
	return 0
}

func (c *conn) dataRow(r []db.Value, kinds []string, formats []int16) {
	b := binary.BigEndian.AppendUint16(nil, uint16(len(r)))
	for i, v := range r {
		if v == nil {
			b = binary.BigEndian.AppendUint32(b, 0xffffffff)
			continue
		}
		var enc []byte
		if format(formats, i) == 1 {
			enc = encodeBinary(v, kinds[i])
		} else {
			enc = []byte(db.Text(v))
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(enc)))
		b = append(b, enc...)
	}
	c.msg('D', b)
}

func encodeBinary(v db.Value, k string) []byte {
	cv, err := db.Coerce(v, k)
	if err != nil {
		cv = v
	}
	switch x := cv.(type) {
	case int64:
		return binary.BigEndian.AppendUint64(nil, uint64(x))
	case float64:
		return binary.BigEndian.AppendUint64(nil, math.Float64bits(x))
	case bool:
		if x {
			return []byte{1}
		}
		return []byte{0}
	}
	return []byte(db.Text(cv))
}

func (c *conn) parse(b []byte) {
	r := &buf{b: b}
	name, q := r.cstr(), r.cstr()
	n := int(r.u16())
	p := &prepared{text: q}
	for i := 0; i < n; i++ {
		p.params = append(p.params, int32(r.u32()))
	}
	stmts, texts, nparams, err := sql.Parse(q)
	if err != nil {
		c.fail(err)
		c.failed = true
		return
	}
	if len(stmts) > 1 {
		c.fail(&db.Error{Code: "42601", Msg: "cannot insert multiple commands into a prepared statement"})
		c.failed = true
		return
	}
	if len(stmts) == 0 {
		p.empty = true
	} else {
		p.stmt, p.text = stmts[0], texts[0]
		inferred := c.s.ParamKinds(p.stmt, nparams)
		for len(p.params) < nparams {
			p.params = append(p.params, 0)
		}
		for i := range p.params {
			if p.params[i] == 0 {
				p.params[i], _ = typeOID(inferred[i])
			}
		}
		if p.cols, p.kinds, err = c.s.Describe(p.stmt); err != nil {
			c.fail(err)
			c.failed = true
			return
		}
	}
	c.stmts[name] = p
	c.msg('1', nil)
}

func (c *conn) bind(b []byte) {
	r := &buf{b: b}
	portalName, stmtName := r.cstr(), r.cstr()
	p := c.stmts[stmtName]
	if p == nil {
		c.fail(&db.Error{Code: "26000", Msg: fmt.Sprintf("prepared statement \"%s\" does not exist", stmtName)})
		c.failed = true
		return
	}
	nf := int(r.u16())
	pf := make([]int16, nf)
	for i := range pf {
		pf[i] = int16(r.u16())
	}
	np := int(r.u16())
	pt := &portal{p: p}
	for i := 0; i < np; i++ {
		ln := int32(r.u32())
		if ln < 0 {
			pt.params = append(pt.params, nil)
			continue
		}
		raw := r.take(int(ln))
		var o int32
		if i < len(p.params) {
			o = p.params[i]
		}
		v, err := decodeParam(raw, format(pf, i), o)
		if err != nil {
			c.fail(err)
			c.failed = true
			return
		}
		pt.params = append(pt.params, v)
	}
	nr := int(r.u16())
	for i := 0; i < nr; i++ {
		pt.formats = append(pt.formats, int16(r.u16()))
	}
	if r.err {
		c.fail(&db.Error{Code: "08P01", Msg: "malformed Bind message"})
		c.failed = true
		return
	}
	c.portals[portalName] = pt
	c.msg('2', nil)
}

func decodeParam(raw []byte, f int16, o int32) (db.Value, error) {
	if f == 0 {
		s := string(raw)
		switch o {
		case 20, 21, 23:
			return db.Coerce(s, "int")
		case 700, 701, 1700:
			return db.Coerce(s, "float")
		case 16:
			return db.Coerce(s, "bool")
		}
		return s, nil
	}
	switch o {
	case 20:
		if len(raw) == 8 {
			return int64(binary.BigEndian.Uint64(raw)), nil
		}
	case 23:
		if len(raw) == 4 {
			return int64(int32(binary.BigEndian.Uint32(raw))), nil
		}
	case 21:
		if len(raw) == 2 {
			return int64(int16(binary.BigEndian.Uint16(raw))), nil
		}
	case 701:
		if len(raw) == 8 {
			return math.Float64frombits(binary.BigEndian.Uint64(raw)), nil
		}
	case 700:
		if len(raw) == 4 {
			return float64(math.Float32frombits(binary.BigEndian.Uint32(raw))), nil
		}
	case 16:
		if len(raw) == 1 {
			return raw[0] != 0, nil
		}
	default:
		return string(raw), nil
	}
	return nil, &db.Error{Code: "22P03", Msg: "incorrect binary data format in bind parameter"}
}

func (c *conn) describe(b []byte) {
	name := string(trimZero(b[1:]))
	if b[0] == 'S' {
		p := c.stmts[name]
		if p == nil {
			c.fail(&db.Error{Code: "26000", Msg: fmt.Sprintf("prepared statement \"%s\" does not exist", name)})
			c.failed = true
			return
		}
		pd := binary.BigEndian.AppendUint16(nil, uint16(len(p.params)))
		for _, o := range p.params {
			pd = binary.BigEndian.AppendUint32(pd, uint32(o))
		}
		c.msg('t', pd)
		if p.cols == nil {
			c.msg('n', nil)
		} else {
			c.rowDesc(p.cols, p.kinds, nil)
		}
		return
	}
	pt := c.portals[name]
	if pt == nil {
		c.fail(&db.Error{Code: "34000", Msg: fmt.Sprintf("portal \"%s\" does not exist", name)})
		c.failed = true
		return
	}
	if pt.p.cols == nil {
		c.msg('n', nil)
	} else {
		c.rowDesc(pt.p.cols, pt.p.kinds, pt.formats)
	}
}

func (c *conn) execute(b []byte) {
	r := &buf{b: b}
	name := r.cstr()
	max := int(int32(r.u32()))
	pt := c.portals[name]
	if pt == nil {
		c.fail(&db.Error{Code: "34000", Msg: fmt.Sprintf("portal \"%s\" does not exist", name)})
		c.failed = true
		return
	}
	if pt.p.empty {
		c.msg('I', nil)
		return
	}
	if pt.res == nil {
		res, err := c.s.Run(pt.p.stmt, pt.p.text, pt.params)
		if err != nil {
			c.fail(err)
			c.failed = true
			return
		}
		pt.res = res
	}
	res := pt.res
	kinds := pt.p.kinds
	if len(kinds) != len(res.Cols) {
		kinds = res.Kinds
	}
	for pt.pos < len(res.Rows) {
		c.dataRow(res.Rows[pt.pos], kinds, pt.formats)
		pt.pos++
		if max > 0 {
			max--
			if max == 0 && pt.pos < len(res.Rows) {
				c.msg('s', nil)
				return
			}
		}
	}
	c.msg('C', str0(nil, res.Tag))
}

type buf struct {
	b   []byte
	err bool
}

func (r *buf) take(n int) []byte {
	if n > len(r.b) {
		r.err = true
		n = len(r.b)
	}
	x := r.b[:n]
	r.b = r.b[n:]
	return x
}

func (r *buf) u16() uint16 {
	x := r.take(2)
	if len(x) < 2 {
		return 0
	}
	return binary.BigEndian.Uint16(x)
}

func (r *buf) u32() uint32 {
	x := r.take(4)
	if len(x) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(x)
}

func (r *buf) cstr() string {
	for i, x := range r.b {
		if x == 0 {
			s := string(r.b[:i])
			r.b = r.b[i+1:]
			return s
		}
	}
	r.err = true
	return ""
}
