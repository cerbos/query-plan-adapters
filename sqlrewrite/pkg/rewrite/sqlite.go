// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package rewrite

import (
	"fmt"
	"sort"
	"strings"

	"entgo.io/ent/dialect"
	sqlite "github.com/rqlite/sql"
)

var sqliteReaders = []string{"sqlite_stat1", "sqlite_stat3", "sqlite_stat4", "dbstat", "eval"}

type sqliteDialect struct{}

func (sqliteDialect) entDialect() string { return dialect.SQLite }

func sqliteParse(text string) (*sqlite.SelectStatement, error) {
	stmts, err := sqlite.NewParser(strings.NewReader(text)).ParseStatements()
	if err != nil {
		return nil, refuse(CodeUnsupportedSQL, "parse: %v", err)
	}
	if len(stmts) != 1 {
		return nil, refuse(CodeUnsupportedSQL, "expected exactly one statement, got %d", len(stmts))
	}
	sel, ok := stmts[0].(*sqlite.SelectStatement)
	if !ok {
		return nil, refuse(CodeUnsupportedSQL, "only SELECT statements are rewritten")
	}
	return sel, nil
}

func ident(i *sqlite.Ident) string {
	if i == nil {
		return ""
	}
	return i.Name
}

type sqliteVisitor struct {
	job     *rewriteJob
	named   map[string]string
	filters map[string]string
	values  map[string]any
	callers int
}

func (v *sqliteVisitor) Visit(n sqlite.Node) (sqlite.Visitor, sqlite.Node, error) {
	switch t := n.(type) {
	case *sqlite.WithClause:
		for _, cte := range t.CTEs {
			if err := v.job.checkCTEName(ident(cte.TableName)); err != nil {
				return nil, nil, err
			}
		}
	case *sqlite.Call:
		if err := sqliteCheckReader(v.job, ident(t.Schema), ident(t.Name)); err != nil {
			return nil, nil, err
		}
	case *sqlite.QualifiedTableFunctionName:
		if err := sqliteCheckReader(v.job, ident(t.Schema), ident(t.Name)); err != nil {
			return nil, nil, err
		}
		if strings.EqualFold(ident(t.Alias), wrapperAlias) {
			return nil, nil, refuse(CodeUnsupportedSQL, "the alias %s is reserved", wrapperAlias)
		}
	case *sqlite.BindExpr:
		if t.Name != "?" {
			return nil, nil, refuse(CodeUnsupportedSQL, "only ? placeholders are supported, found %s", t.Name)
		}
		v.callers++
	case *sqlite.BinaryExpr:
		if t.Op == sqlite.IN || t.Op == sqlite.NOTIN {
			if err := sqliteCheckInTable(v.job, t.Y); err != nil {
				return nil, nil, err
			}
		}
	case *sqlite.ParenSource:
		if strings.EqualFold(ident(t.Alias), wrapperAlias) {
			return nil, nil, refuse(CodeUnsupportedSQL, "the alias %s is reserved", wrapperAlias)
		}
	case *sqlite.QualifiedTableName:
		if strings.EqualFold(ident(t.Alias), wrapperAlias) {
			return nil, nil, refuse(CodeUnsupportedSQL, "the alias %s is reserved", wrapperAlias)
		}
		schema, name := ident(t.Schema), ident(t.Name)
		if err := sqliteCheckReader(v.job, schema, name); err != nil {
			return nil, nil, err
		}
		target, err := v.job.target(schema, name)
		if err != nil {
			return nil, nil, err
		}
		if target == nil || target.kind == KindAlwaysAllowed {
			return v, n, nil
		}
		filter, ok := v.named[target.res.kind]
		if !ok {
			filter, err = sqliteNameBinds(target, len(v.named), v.values)
			if err != nil {
				return nil, nil, err
			}
			v.named[target.res.kind] = filter
		}
		marker := filterMarker(len(v.filters))
		v.filters[marker] = filter
		inner, err := sqliteParse(wrapperSkeleton(marker))
		if err != nil {
			return nil, nil, err
		}
		qt := t.Clone()
		qt.Alias = &sqlite.Ident{Name: wrapperAlias}
		qt.As = sqlite.Pos{}
		inner.Source = qt
		alias := t.Alias
		if alias == nil {
			alias = &sqlite.Ident{Name: name, Quoted: t.Name.Quoted}
		}
		return nil, &sqlite.ParenSource{X: inner, Alias: alias}, nil
	}
	return v, n, nil
}

// sqliteCheckInTable refuses `x IN table`, which reads a table outside any FROM clause.
func sqliteCheckInTable(j *rewriteJob, y sqlite.Expr) error {
	var schema, name string
	switch y := y.(type) {
	case *sqlite.Ident:
		name = y.Name
	case *sqlite.QualifiedRef:
		schema, name = ident(y.Table), ident(y.Column)
	default:
		return nil
	}
	if err := sqliteCheckReader(j, schema, name); err != nil {
		return err
	}
	if j.isProtected(schema, name) {
		return refuse(CodeUnsupportedSQL, "IN %s reads a protected table outside a FROM clause", name)
	}
	return nil
}

func (v *sqliteVisitor) VisitEnd(n sqlite.Node) (sqlite.Node, error) { return n, nil }

func (sqliteDialect) rewrite(j *rewriteJob) (string, []any, error) {
	sel, err := sqliteParse(j.sql)
	if err != nil {
		return "", nil, err
	}
	v := &sqliteVisitor{job: j, named: map[string]string{}, filters: map[string]string{}, values: map[string]any{}}
	node, err := sqlite.Walk(v, sel)
	if err != nil {
		return "", nil, err
	}
	if v.callers > len(j.params) {
		return "", nil, refuse(CodeInvalidRequest, "the statement has %d ? placeholders but %d params were sent", v.callers, len(j.params))
	}
	text, err := spliceFilters(node.String(), v.filters)
	if err != nil {
		return "", nil, err
	}

	out, err := sqliteParse(text)
	if err != nil {
		return "", nil, refuse(CodeUnsupportedSQL, "verify: the rewritten statement does not parse: %v", err)
	}
	type bind struct {
		name   string
		offset int
	}
	var binds []bind
	exempt := map[*sqlite.QualifiedTableName]bool{}
	var verr error
	_, _ = sqlite.Walk(sqliteFunc(func(n sqlite.Node) bool {
		switch t := n.(type) {
		case *sqlite.BindExpr:
			binds = append(binds, bind{name: t.Name, offset: t.NamePos.Offset})
		case *sqlite.ParenSource:
			if s, ok := t.X.(*sqlite.SelectStatement); ok && s.WhereExpr != nil {
				if q, ok := s.Source.(*sqlite.QualifiedTableName); ok && ident(q.Alias) == wrapperAlias {
					exempt[q] = true
				}
			}
		case *sqlite.QualifiedTableName:
			if !exempt[t] && j.mustBeWrapped(ident(t.Schema), ident(t.Name)) {
				verr = refuse(CodeUnsupportedSQL, "verify: %s is read outside a filter", ident(t.Name))
				return false
			}
		}
		return true
	}), out)
	if verr != nil {
		return "", nil, verr
	}

	sort.Slice(binds, func(a, b int) bool { return binds[a].offset < binds[b].offset })
	params := make([]any, 0, len(binds))
	var sb strings.Builder
	last, caller := 0, 0
	for _, b := range binds {
		sb.WriteString(text[last:b.offset])
		sb.WriteString("?")
		last = b.offset + len(b.name)
		if b.name == "?" {
			params = append(params, j.params[caller])
			caller++
			continue
		}
		val, ok := v.values[b.name]
		if !ok {
			return "", nil, refuse(CodeUnsupportedSQL, "verify: placeholder %s has no value", b.name)
		}
		params = append(params, val)
	}
	sb.WriteString(text[last:])
	return sb.String(), params, nil
}

type sqliteFunc func(sqlite.Node) bool

func (f sqliteFunc) Visit(n sqlite.Node) (sqlite.Visitor, sqlite.Node, error) {
	if !f(n) {
		return nil, n, nil
	}
	return f, n, nil
}

func (f sqliteFunc) VisitEnd(n sqlite.Node) (sqlite.Node, error) { return n, nil }

func sqliteCheckReader(j *rewriteJob, schema, name string) error {
	for _, r := range sqliteReaders {
		if strings.EqualFold(r, name) {
			return refuse(CodeUnsupportedSQL, "%s can read a protected table", name)
		}
	}
	return j.checkReader(schema, name)
}

// sqliteNameBinds names the filter's ? placeholders :cerbos_<n>_<k>, at the offsets the parser
// reports, so their values can be ordered once the filter sits among the caller's placeholders.
func sqliteNameBinds(w *wrapper, n int, values map[string]any) (string, error) {
	const prefix = "SELECT 1 WHERE "
	sel, err := sqliteParse(prefix + w.text)
	if err != nil {
		return "", refuse(CodeUnsupportedPlan, "%s: rendered filter does not parse: %v", w.res.kind, err)
	}
	var offsets []int
	_, _ = sqlite.Walk(sqliteFunc(func(node sqlite.Node) bool {
		if b, ok := node.(*sqlite.BindExpr); ok {
			offsets = append(offsets, b.NamePos.Offset-len(prefix))
		}
		return true
	}), sel)
	if len(offsets) != len(w.args) {
		return "", refuse(CodeUnsupportedPlan, "%s: rendered filter has %d placeholders for %d args", w.res.kind, len(offsets), len(w.args))
	}
	sort.Ints(offsets)
	var sb strings.Builder
	last := 0
	for k, off := range offsets {
		if w.text[off] != '?' {
			return "", refuse(CodeUnsupportedPlan, "%s: rendered filter has an unexpected placeholder", w.res.kind)
		}
		name := fmt.Sprintf(":cerbos_%d_%d", n, k+1)
		values[name] = w.args[k]
		sb.WriteString(w.text[last:off])
		sb.WriteString(name)
		last = off + 1
	}
	sb.WriteString(w.text[last:])
	return sb.String(), nil
}
