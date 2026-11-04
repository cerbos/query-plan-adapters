// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package rewrite

import (
	"strconv"
	"strings"

	"entgo.io/ent/dialect"
	"vitess.io/vitess/go/vt/sqlparser"
)

var mysqlReaders = []string{"column_statistics", "innodb_ft_index_table", "innodb_ft_index_cache"}

type mysqlDialect struct{}

func (mysqlDialect) entDialect() string { return dialect.MySQL }

func mysqlParser() (*sqlparser.Parser, error) {
	return sqlparser.New(sqlparser.Options{MySQLServerVersion: "8.4.0"})
}

func (mysqlDialect) rewrite(j *rewriteJob) (string, []any, error) {
	parser, err := mysqlParser()
	if err != nil {
		return "", nil, err
	}
	stmt, _, err := parser.Parse2(j.sql)
	if err != nil {
		return "", nil, refuse(CodeUnsupportedSQL, "parse: %v", err)
	}
	switch stmt.(type) {
	case *sqlparser.Select, *sqlparser.Union:
	default:
		return "", nil, refuse(CodeUnsupportedSQL, "only SELECT statements are rewritten")
	}

	if err := mysqlRoundTrip(parser, stmt); err != nil {
		return "", nil, err
	}

	values := map[string]any{}
	filters := map[string]string{}
	filterArgs := map[string][]any{}
	var walkErr error
	fail := func(err error) bool {
		walkErr = err
		return false
	}
	result := sqlparser.Rewrite(stmt, func(c *sqlparser.Cursor) bool {
		if walkErr != nil {
			return false
		}
		switch n := c.Node().(type) {
		case *sqlparser.Select:
			if n.Into != nil {
				return fail(refuse(CodeUnsupportedSQL, "SELECT INTO is not rewritten"))
			}
		case *sqlparser.With:
			for _, cte := range n.CTEs {
				if err := j.checkCTEName(cte.ID.String()); err != nil {
					return fail(err)
				}
			}
		case *sqlparser.FuncExpr:
			if err := mysqlCheckReader(j, n.Qualifier.String(), n.Name.String()); err != nil {
				return fail(err)
			}
		case *sqlparser.Argument:
			idx, err := strconv.Atoi(strings.TrimPrefix(n.Name, "v"))
			if err != nil || !strings.HasPrefix(n.Name, "v") {
				return fail(refuse(CodeUnsupportedSQL, "named parameter :%s is not MySQL syntax", n.Name))
			}
			if idx > len(j.params) {
				return fail(refuse(CodeInvalidRequest, "the statement has %d or more ? placeholders but %d params were sent", idx, len(j.params)))
			}
			values[n.Name] = j.params[idx-1]
		case *sqlparser.AliasedTableExpr:
			if strings.EqualFold(n.As.String(), wrapperAlias) {
				return fail(refuse(CodeUnsupportedSQL, "the alias %s is reserved", wrapperAlias))
			}
			tn, ok := n.Expr.(sqlparser.TableName)
			if !ok {
				return true
			}
			if err := mysqlCheckReader(j, tn.Qualifier.String(), tn.Name.String()); err != nil {
				return fail(err)
			}
			target, err := j.target(tn.Qualifier.String(), tn.Name.String())
			if err != nil {
				return fail(err)
			}
			if target == nil || target.kind == KindAlwaysAllowed {
				return true
			}
			marker := filterMarker(len(filters))
			filters[marker], filterArgs[marker] = target.text, target.args
			parsed, err := parser.Parse(wrapperSkeleton(marker))
			if err != nil {
				return fail(err)
			}
			inner := parsed.(*sqlparser.Select)                      //nolint:forcetypeassert
			innerFrom := inner.From[0].(*sqlparser.AliasedTableExpr) //nolint:forcetypeassert
			innerFrom.Expr = tn
			innerFrom.Hints = n.Hints
			innerFrom.Partitions = n.Partitions
			as := n.As
			if as.IsEmpty() {
				as = tn.Name
			}
			c.Replace(&sqlparser.AliasedTableExpr{Expr: &sqlparser.DerivedTable{Select: inner}, As: as, Columns: n.Columns})
			return false
		}
		return true
	}, nil)
	if walkErr != nil {
		return "", nil, walkErr
	}

	var params []any
	var printErr error
	buf := sqlparser.NewTrackedBuffer(func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch n := node.(type) {
		case *sqlparser.Argument:
			v, ok := values[n.Name]
			if !ok {
				printErr = refuse(CodeUnsupportedSQL, "verify: placeholder %s has no value", n.Name)
			}
			params = append(params, v)
			buf.WriteString("?")
			return
		case *sqlparser.ColName:
			if args, ok := filterArgs[n.Name.String()]; ok && n.Qualifier.IsEmpty() {
				params = append(params, args...)
			}
		}
		node.Format(buf)
	})
	buf.Myprintf("%v", result)
	if printErr != nil {
		return "", nil, printErr
	}
	out, err := spliceFilters(buf.String(), filters)
	if err != nil {
		return "", nil, err
	}
	if err := mysqlVerify(j, parser, out, len(params)); err != nil {
		return "", nil, err
	}
	return out, params, nil
}

func mysqlCheckReader(j *rewriteJob, schema, name string) error {
	for _, r := range mysqlReaders {
		if strings.EqualFold(r, name) {
			return refuse(CodeUnsupportedSQL, "%s can read a protected table", name)
		}
	}
	return j.checkReader(schema, name)
}

// mysqlRoundTrip refuses a statement the printer does not print back to the same tree.
func mysqlRoundTrip(parser *sqlparser.Parser, stmt sqlparser.Statement) error {
	again, err := parser.Parse(sqlparser.String(stmt))
	if err != nil {
		return refuse(CodeUnsupportedSQL, "the statement does not survive a print and re-parse: %v", err)
	}
	if !sqlparser.Equals.SQLNode(stmt, again) {
		return refuse(CodeUnsupportedSQL, "the statement does not survive a print and re-parse unchanged")
	}
	return nil
}

func mysqlVerify(j *rewriteJob, parser *sqlparser.Parser, out string, nParams int) error {
	stmt, bv, err := parser.Parse2(out)
	if err != nil {
		return refuse(CodeUnsupportedSQL, "verify: the rewritten statement does not parse: %v", err)
	}
	if len(bv) != nParams {
		return refuse(CodeUnsupportedSQL, "verify: %d placeholders but %d params", len(bv), nParams)
	}
	exempt := map[*sqlparser.AliasedTableExpr]bool{}
	var verr error
	sqlparser.Rewrite(stmt, func(c *sqlparser.Cursor) bool {
		n, ok := c.Node().(*sqlparser.AliasedTableExpr)
		if !ok {
			return true
		}
		if dt, ok := n.Expr.(*sqlparser.DerivedTable); ok {
			if sel, ok := dt.Select.(*sqlparser.Select); ok && len(sel.From) == 1 && sel.Where != nil {
				if inner, ok := sel.From[0].(*sqlparser.AliasedTableExpr); ok && inner.As.String() == wrapperAlias {
					exempt[inner] = true
				}
			}
			return true
		}
		tn, ok := n.Expr.(sqlparser.TableName)
		if ok && !exempt[n] && j.mustBeWrapped(tn.Qualifier.String(), tn.Name.String()) {
			verr = refuse(CodeUnsupportedSQL, "verify: %s is read outside a filter", tn.Name.String())
			return false
		}
		return true
	}, nil)
	return verr
}
