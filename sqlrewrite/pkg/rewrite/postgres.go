// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package rewrite

import (
	"strings"

	"entgo.io/ent/dialect"
	pg "github.com/pganalyze/pg_query_go/v6"
	pgquery "github.com/wasilibs/go-pgquery"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var postgresReaders = []string{
	"query_to_xml", "query_to_xmlschema", "query_to_xml_and_xmlschema",
	"table_to_xml", "table_to_xmlschema", "table_to_xml_and_xmlschema",
	"cursor_to_xml", "cursor_to_xmlschema",
	"schema_to_xml", "schema_to_xmlschema", "schema_to_xml_and_xmlschema",
	"database_to_xml", "database_to_xmlschema", "database_to_xml_and_xmlschema",
	"dblink", "dblink_exec", "dblink_open", "dblink_fetch", "dblink_send_query", "dblink_get_result",
	"ts_stat", "pg_stats", "pg_stats_ext", "pg_stats_ext_exprs", "pg_statistic", "pg_statistic_ext_data",
}

type postgresDialect struct{}

func (postgresDialect) entDialect() string { return dialect.Postgres }

func (postgresDialect) rewrite(j *rewriteJob) (string, []any, error) {
	tree, err := pgquery.Parse(j.sql)
	if err != nil {
		return "", nil, refuse(CodeUnsupportedSQL, "parse: %v", err)
	}
	stmts := tree.GetStmts()
	if len(stmts) != 1 {
		return "", nil, refuse(CodeUnsupportedSQL, "expected exactly one statement, got %d", len(stmts))
	}
	sel := stmts[0].GetStmt().GetSelectStmt()
	if sel == nil {
		return "", nil, refuse(CodeUnsupportedSQL, "only SELECT statements are rewritten")
	}

	w := &pgWalker{job: j, from: map[*pg.Node]bool{}}
	if err := w.collect(tree); err != nil {
		return "", nil, err
	}
	if w.maxParam > len(j.params) {
		return "", nil, refuse(CodeInvalidRequest, "the statement uses $%d but %d params were sent", w.maxParam, len(j.params))
	}

	if err := postgresRoundTrip(tree); err != nil {
		return "", nil, err
	}

	offset := len(j.params)
	params := append([]any(nil), j.params...)
	rendered := map[string]string{}
	filters := map[string]string{}
	for i, rep := range w.replacements {
		if rep.wrapper.kind == KindAlwaysAllowed {
			continue
		}
		filter, ok := rendered[rep.wrapper.res.kind]
		if !ok {
			var args []any
			filter, args, err = renderFilter(rep.wrapper.expr, dialect.Postgres, offset)
			if err != nil {
				return "", nil, refuse(CodeUnsupportedPlan, "%s: %v", rep.wrapper.res.kind, err)
			}
			offset += len(args)
			params = append(params, args...)
			rendered[rep.wrapper.res.kind] = filter
		}
		marker := filterMarker(i)
		filters[marker] = filter
		skeleton, err := pgquery.Parse(wrapperSkeleton(marker))
		if err != nil {
			return "", nil, err
		}
		inner := skeleton.GetStmts()[0].GetStmt()
		from := inner.GetSelectStmt().GetFromClause()
		rv := proto.Clone(rep.rangeVar).(*pg.RangeVar) //nolint:forcetypeassert
		alias := rv.GetAlias()
		rv.Alias = &pg.Alias{Aliasname: wrapperAlias}
		from[0] = &pg.Node{Node: &pg.Node_RangeVar{RangeVar: rv}}
		if alias == nil {
			alias = &pg.Alias{Aliasname: rv.GetRelname()}
		}
		rep.node.Node = &pg.Node_RangeSubselect{RangeSubselect: &pg.RangeSubselect{Subquery: inner, Alias: alias}}
	}

	printed, err := pgquery.Deparse(tree)
	if err != nil {
		return "", nil, refuse(CodeUnsupportedSQL, "deparse: %v", err)
	}
	out, err := spliceFilters(printed, filters)
	if err != nil {
		return "", nil, err
	}
	if err := postgresVerify(j, out, len(params)); err != nil {
		return "", nil, err
	}
	return out, params, nil
}

// postgresRoundTrip refuses a statement the deparser does not print back to the same tree.
func postgresRoundTrip(tree *pg.ParseResult) error {
	printed, err := pgquery.Deparse(tree)
	if err != nil {
		return refuse(CodeUnsupportedSQL, "deparse: %v", err)
	}
	again, err := pgquery.Parse(printed)
	if err != nil {
		return refuse(CodeUnsupportedSQL, "the statement does not survive a print and re-parse: %v", err)
	}
	if !proto.Equal(withoutLocations(tree), withoutLocations(again)) {
		return refuse(CodeUnsupportedSQL, "the statement does not survive a print and re-parse unchanged")
	}
	return nil
}

func withoutLocations(tree *pg.ParseResult) *pg.ParseResult {
	c := proto.Clone(tree).(*pg.ParseResult) //nolint:forcetypeassert
	_ = walkProto(c.ProtoReflect(), func(m protoreflect.Message) error {
		fields := m.Descriptor().Fields()
		for i := range fields.Len() {
			if fd := fields.Get(i); fd.Kind() == protoreflect.Int32Kind && !fd.IsList() {
				switch fd.Name() {
				case "location", "stmt_location", "stmt_len":
					m.Clear(fd)
				}
			}
		}
		return nil
	})
	return c
}

type pgReplacement struct {
	node     *pg.Node
	rangeVar *pg.RangeVar
	wrapper  *wrapper
}

type pgWalker struct {
	job          *rewriteJob
	from         map[*pg.Node]bool
	replacements []pgReplacement
	maxParam     int
}

func (w *pgWalker) collect(tree *pg.ParseResult) error {
	err := walkProto(tree.ProtoReflect(), func(m protoreflect.Message) error {
		switch n := m.Interface().(type) {
		case *pg.SelectStmt:
			if n.GetIntoClause() != nil {
				return refuse(CodeUnsupportedSQL, "SELECT INTO is not rewritten")
			}
			for _, item := range n.GetFromClause() {
				w.from[item] = true
			}
			for _, cte := range n.GetWithClause().GetCtes() {
				c := cte.GetCommonTableExpr()
				if err := w.job.checkCTEName(c.GetCtename()); err != nil {
					return err
				}
				if c.GetCtequery().GetSelectStmt() == nil {
					return refuse(CodeUnsupportedSQL, "the CTE %s is not a SELECT", c.GetCtename())
				}
			}
		case *pg.Alias:
			if strings.EqualFold(n.GetAliasname(), wrapperAlias) {
				return refuse(CodeUnsupportedSQL, "the alias %s is reserved", wrapperAlias)
			}
		case *pg.JoinExpr:
			w.from[n.GetLarg()] = true
			w.from[n.GetRarg()] = true
		case *pg.ParamRef:
			if int(n.GetNumber()) > w.maxParam {
				w.maxParam = int(n.GetNumber())
			}
		case *pg.FuncCall:
			names := n.GetFuncname()
			if len(names) > 0 {
				name := names[len(names)-1].GetString_().GetSval()
				schema := ""
				if len(names) > 1 {
					schema = names[len(names)-2].GetString_().GetSval()
				}
				if err := w.checkReader(schema, name); err != nil {
					return err
				}
			}
		case *pg.Node:
			rv := n.GetRangeVar()
			if rv == nil {
				return nil
			}
			if err := w.checkReader(rv.GetSchemaname(), rv.GetRelname()); err != nil {
				return err
			}
			if !w.from[n] {
				if w.job.isProtected(rv.GetSchemaname(), rv.GetRelname()) {
					return refuse(CodeUnsupportedSQL, "%s appears outside a FROM clause", rv.GetRelname())
				}
				return nil
			}
			if rv.GetCatalogname() != "" && w.job.isProtected(rv.GetSchemaname(), rv.GetRelname()) {
				return refuse(CodeAmbiguousReference, "%s.%s.%s names another database", rv.GetCatalogname(), rv.GetSchemaname(), rv.GetRelname())
			}
			target, err := w.job.target(rv.GetSchemaname(), rv.GetRelname())
			if err != nil {
				return err
			}
			if target != nil {
				w.replacements = append(w.replacements, pgReplacement{node: n, rangeVar: rv, wrapper: target})
			}
		case *pg.RangeTableSample:
			rv := n.GetRelation().GetRangeVar()
			if rv != nil && w.job.isProtected(rv.GetSchemaname(), rv.GetRelname()) {
				return refuse(CodeUnsupportedSQL, "TABLESAMPLE on the protected table %s", rv.GetRelname())
			}
			if rv != nil {
				w.from[n.GetRelation()] = false
			}
		}
		return nil
	})
	return err
}

func (w *pgWalker) checkReader(schema, name string) error {
	for _, r := range postgresReaders {
		if strings.EqualFold(r, name) {
			return refuse(CodeUnsupportedSQL, "%s can read a protected table", name)
		}
	}
	return w.job.checkReader(schema, name)
}

func postgresVerify(j *rewriteJob, out string, nParams int) error {
	tree, err := pgquery.Parse(out)
	if err != nil {
		return refuse(CodeUnsupportedSQL, "verify: the rewritten statement does not parse: %v", err)
	}
	exempt := map[*pg.RangeVar]bool{}
	return walkProto(tree.ProtoReflect(), func(m protoreflect.Message) error {
		switch n := m.Interface().(type) {
		case *pg.RangeSubselect:
			if sel := n.GetSubquery().GetSelectStmt(); isPostgresWrapper(sel) {
				exempt[sel.GetFromClause()[0].GetRangeVar()] = true
			}
		case *pg.ParamRef:
			if int(n.GetNumber()) > nParams {
				return refuse(CodeUnsupportedSQL, "verify: $%d has no value", n.GetNumber())
			}
		case *pg.RangeVar:
			if !exempt[n] && j.mustBeWrapped(n.GetSchemaname(), n.GetRelname()) {
				return refuse(CodeUnsupportedSQL, "verify: %s is read outside a filter", n.GetRelname())
			}
		}
		return nil
	})
}

func isPostgresWrapper(sel *pg.SelectStmt) bool {
	from := sel.GetFromClause()
	if len(from) != 1 {
		return false
	}
	rv := from[0].GetRangeVar()
	return rv != nil && rv.GetAlias().GetAliasname() == wrapperAlias && sel.GetWhereClause() != nil
}

func walkProto(m protoreflect.Message, visit func(protoreflect.Message) error) error {
	if err := visit(m); err != nil {
		return err
	}
	var err error
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		err = eachMessage(fd, v, func(child protoreflect.Message) error { return walkProto(child, visit) })
		return err == nil
	})
	return err
}

func eachMessage(fd protoreflect.FieldDescriptor, v protoreflect.Value, f func(protoreflect.Message) error) error {
	switch {
	case fd.IsList() && fd.Kind() == protoreflect.MessageKind:
		l := v.List()
		for i := range l.Len() {
			if err := f(l.Get(i).Message()); err != nil {
				return err
			}
		}
	case fd.IsMap():
		return nil
	case fd.Kind() == protoreflect.MessageKind:
		return f(v.Message())
	}
	return nil
}
