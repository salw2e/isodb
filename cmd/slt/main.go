// slt runs sqllogictest files against an in-memory isodb and reports how many records pass.
package main

import (
	"bufio"
	"crypto/md5"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"isodb/db"
	"isodb/sql"
)

type record struct {
	line       int
	kind       string // statement or query
	ok         bool   // statement ok
	sql        string
	types      string
	sortMode   string
	label      string
	expected   []string
	skip       bool
	halt       bool
	hashThresh int
}

func main() {
	verbose := flag.Bool("v", false, "print failures")
	prof := flag.String("cpuprofile", "", "write a CPU profile")
	flag.BoolVar(&disk, "disk", false, "run each file against a data directory instead of memory")
	flag.Parse()
	if *prof != "" {
		f, _ := os.Create(*prof)
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	total, pass := 0, 0
	start := time.Now()
	for _, f := range flag.Args() {
		t, p := runFile(f, *verbose)
		total += t
		pass += p
		fmt.Printf("%-60s %6d/%-6d\n", f, p, t)
	}
	pct := 0.0
	if total > 0 {
		pct = 100 * float64(pass) / float64(total)
	}
	fmt.Printf("TOTAL %d/%d passed (%.3f%%) in %s\n", pass, total, pct, time.Since(start).Round(time.Second))
}

func parseFile(path string) ([]record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	var out []record
	var lines []string
	n := 0
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	thresh := 0
	for n < len(lines) {
		l := strings.TrimSpace(lines[n])
		if l == "" || strings.HasPrefix(l, "#") {
			n++
			continue
		}
		r := record{line: n + 1}
		for {
			f := strings.Fields(l)
			if len(f) >= 2 && f[0] == "skipif" && f[1] == "postgresql" {
				r.skip = true
			} else if len(f) >= 2 && f[0] == "onlyif" && f[1] != "postgresql" {
				r.skip = true
			} else if len(f) == 0 || f[0] != "skipif" && f[0] != "onlyif" {
				break
			}
			n++
			l = strings.TrimSpace(lines[n])
		}
		f := strings.Fields(l)
		n++
		switch f[0] {
		case "hash-threshold":
			thresh, _ = strconv.Atoi(f[1])
			continue
		case "halt":
			if !r.skip {
				return out, nil
			}
			continue
		case "statement":
			r.kind, r.ok = "statement", f[1] == "ok"
			var q []string
			for n < len(lines) && strings.TrimSpace(lines[n]) != "" {
				q = append(q, lines[n])
				n++
			}
			r.sql = strings.Join(q, "\n")
		case "query":
			r.kind = "query"
			r.types = f[1]
			r.sortMode = "nosort"
			if len(f) > 2 {
				r.sortMode = f[2]
			}
			if len(f) > 3 {
				r.label = f[3]
			}
			var q []string
			for n < len(lines) && strings.TrimSpace(lines[n]) != "----" && strings.TrimSpace(lines[n]) != "" {
				q = append(q, lines[n])
				n++
			}
			r.sql = strings.Join(q, "\n")
			if n < len(lines) && strings.TrimSpace(lines[n]) == "----" {
				n++
				for n < len(lines) && strings.TrimSpace(lines[n]) != "" {
					r.expected = append(r.expected, strings.TrimSpace(lines[n]))
					n++
				}
			}
		default:
			continue
		}
		r.hashThresh = thresh
		out = append(out, r)
	}
	return out, nil
}

func format(v db.Value, t byte) string {
	if v == nil {
		return "NULL"
	}
	switch t {
	case 'I':
		switch x := v.(type) {
		case int64:
			return strconv.FormatInt(x, 10)
		case float64:
			return strconv.FormatInt(int64(x), 10)
		case bool:
			if x {
				return "1"
			}
			return "0"
		case string:
			n, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
			if err != nil {
				return "0"
			}
			return strconv.FormatInt(int64(n), 10)
		}
	case 'R':
		f, err := db.Coerce(v, "float")
		if err != nil {
			return "0.000"
		}
		x := f.(float64)
		if x == 0 {
			x = math.Abs(x)
		}
		return fmt.Sprintf("%.3f", x)
	}
	s := db.Text(v)
	if b, ok := v.(bool); ok {
		s = "0"
		if b {
			s = "1"
		}
	}
	if s == "" {
		return "(empty)"
	}
	out := []byte(s)
	for i, c := range out {
		if c < ' ' || c > '~' {
			out[i] = '@'
		}
	}
	return string(out)
}

var disk bool

func runFile(path string, verbose bool) (int, int) {
	recs, err := parseFile(path)
	if err != nil {
		fmt.Println(err)
		return 0, 0
	}
	dir := ""
	if disk {
		var err error
		if dir, err = os.MkdirTemp("", "slt"); err != nil {
			fmt.Println(err)
			return 0, 0
		}
		defer os.RemoveAll(dir)
	}
	d, err := db.Open(dir)
	if err != nil {
		fmt.Println(err)
		return 0, 0
	}
	defer d.Close()
	s := d.NewSession("slt")
	labels := map[string]string{}
	total, pass := 0, 0
	for _, r := range recs {
		if r.skip {
			continue
		}
		total++
		t0 := time.Now()
		ok, why := run(s, r, labels)
		if d := time.Since(t0); d > time.Second {
			fmt.Printf("slow %s %s:%d %s\n", d.Round(time.Millisecond), path, r.line, strings.ReplaceAll(r.sql, "\n", " "))
		}
		if ok {
			pass++
		} else if verbose {
			fmt.Printf("%s:%d: %s\n  %s\n", path, r.line, why, strings.ReplaceAll(r.sql, "\n", " "))
		}
	}
	return total, pass
}

func run(s *db.Session, r record, labels map[string]string) (ok bool, why string) {
	defer func() {
		if p := recover(); p != nil {
			ok, why = false, fmt.Sprintf("panic: %v", p)
		}
	}()
	stmts, texts, _, err := sql.Parse(r.sql)
	var res *db.Result
	if err == nil {
		for i, st := range stmts {
			if res, err = s.Run(st, texts[i], nil); err != nil {
				break
			}
		}
	}
	if r.kind == "statement" {
		if (err == nil) != r.ok {
			return false, fmt.Sprintf("statement: got err=%v", err)
		}
		return true, ""
	}
	if err != nil {
		return false, "error: " + err.Error()
	}
	if res == nil {
		return false, "no result"
	}
	var vals []string
	rows := make([][]string, len(res.Rows))
	for i, row := range res.Rows {
		for j, v := range row {
			t := byte('T')
			if j < len(r.types) {
				t = r.types[j]
			}
			rows[i] = append(rows[i], format(v, t))
		}
	}
	switch r.sortMode {
	case "rowsort":
		sort.Slice(rows, func(a, b int) bool { return strings.Join(rows[a], " ") < strings.Join(rows[b], " ") })
	}
	for _, row := range rows {
		vals = append(vals, row...)
	}
	if r.sortMode == "valuesort" {
		sort.Strings(vals)
	}
	var got []string
	if r.hashThresh > 0 && len(vals) > r.hashThresh || len(r.expected) == 1 && strings.Contains(r.expected[0], "values hashing to") {
		h := md5.New()
		for _, v := range vals {
			h.Write([]byte(v))
			h.Write([]byte("\n"))
		}
		got = []string{fmt.Sprintf("%d values hashing to %x", len(vals), h.Sum(nil))}
	} else {
		got = vals
	}
	if r.label != "" {
		g := strings.Join(got, "\n")
		if prev, seen := labels[r.label]; seen && prev != g {
			return false, "label mismatch"
		}
		labels[r.label] = g
	}
	if r.expected == nil && r.label != "" {
		return true, ""
	}
	if strings.Join(got, "\n") != strings.Join(r.expected, "\n") {
		return false, fmt.Sprintf("got %v want %v", trunc(got), trunc(r.expected))
	}
	return true, ""
}

func trunc(v []string) []string {
	if len(v) > 6 {
		return append(v[:6:6], "...")
	}
	return v
}
