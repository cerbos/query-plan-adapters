// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbospgx

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cerbos/query-plan-adapters/pgx/internal/queryplan"
)

// renderer walks the abstract expression tree and emits PostgreSQL text with `$n` placeholders.
// Every value that came from the plan is bound as a parameter — no plan data is ever interpolated
// into the SQL text, so a policy constant cannot become SQL syntax.
type renderer struct {
	sb   strings.Builder
	args []any
	// offset shifts placeholder numbering so the fragment can be appended to a query that
	// already binds arguments. Numbering is applied while writing rather than rewritten
	// afterwards: a regex over finished SQL would also match a `$1` inside a bound string.
	offset int
}

func render(e queryplan.Expr, offset int) (string, []any, error) {
	r := &renderer{offset: offset}
	if err := r.write(e); err != nil {
		return "", nil, err
	}
	return r.sb.String(), r.args, nil
}

func (r *renderer) bind(v any) {
	r.args = append(r.args, v)
	r.sb.WriteString("$")
	r.sb.WriteString(strconv.Itoa(r.offset + len(r.args)))
	r.sb.WriteString(pgTypeSuffix(v))
}

// pgTypeSuffix pins the SQL type of a bound parameter.
//
// PostgreSQL infers an untyped `$n` from the context it appears in, and in an expression such as
// `CAST(col AS double precision) / $1` there is nothing to infer from — it falls back to text and
// the query dies with "operator does not exist: double precision / text". Since CEL numbers are
// doubles and the plan is the only thing that knows a literal's type, every parameter states its
// own type rather than relying on inference.
func pgTypeSuffix(v any) string {
	switch v.(type) {
	case bool:
		return "::boolean"
	case float32, float64:
		return "::double precision"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "::bigint"
	case time.Time:
		return "::timestamptz"
	case string:
		return "::text"
	default:
		return ""
	}
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (r *renderer) writeColumn(c queryplan.Column) {
	if c.Qualifier != "" {
		r.sb.WriteString(quoteIdent(c.Qualifier))
		r.sb.WriteString(".")
	}
	r.sb.WriteString(quoteIdent(c.Name))
}

// write emits one expression node. A single type switch over the tree is clearer here than
// dispatching through a visitor, and each arm is only a few lines.
//
//nolint:gocyclo // See above: the switch is wide but flat.
func (r *renderer) write(e queryplan.Expr) error {
	switch t := e.(type) {
	case queryplan.Column:
		r.writeColumn(t)
		return nil

	case queryplan.Lit:
		if t.V == nil {
			r.sb.WriteString("NULL")
			return nil
		}
		r.bind(t.V)
		return nil

	case queryplan.BoolConst:
		if t.V {
			r.sb.WriteString("TRUE")
		} else {
			r.sb.WriteString("FALSE")
		}
		return nil

	case queryplan.Cmp:
		return r.writeBinary(cmpSymbol(t.Op), t.L, t.R)

	case queryplan.Arith:
		return r.writeBinary(arithSymbol(t.Op), t.L, t.R)

	case queryplan.Logic:
		sep := " OR "
		if t.And {
			sep = " AND "
		}
		r.sb.WriteString("(")
		for i, x := range t.Xs {
			if i > 0 {
				r.sb.WriteString(sep)
			}
			if err := r.write(x); err != nil {
				return err
			}
		}
		r.sb.WriteString(")")
		return nil

	case queryplan.Not:
		r.sb.WriteString("(NOT ")
		if err := r.write(t.X); err != nil {
			return err
		}
		r.sb.WriteString(")")
		return nil

	case queryplan.IsNull:
		r.sb.WriteString("(")
		if err := r.write(t.X); err != nil {
			return err
		}
		if t.Negate {
			r.sb.WriteString(" IS NOT NULL)")
		} else {
			r.sb.WriteString(" IS NULL)")
		}
		return nil

	case queryplan.TruthTest:
		r.sb.WriteString("(")
		if err := r.write(t.X); err != nil {
			return err
		}
		switch t.Want {
		case queryplan.TruthTrue:
			r.sb.WriteString(" IS TRUE)")
		case queryplan.TruthFalse:
			r.sb.WriteString(" IS FALSE)")
		default:
			r.sb.WriteString(" IS NULL)")
		}
		return nil

	case queryplan.Like:
		r.sb.WriteString("(")
		if err := r.write(t.Receiver); err != nil {
			return err
		}
		r.sb.WriteString(" LIKE ")
		if err := r.write(t.Pattern); err != nil {
			return err
		}
		// The escape character is bound rather than inlined so that neither
		// standard_conforming_strings nor backslash_quote can change its meaning.
		r.sb.WriteString(" ESCAPE ")
		r.bind(`\`)
		r.sb.WriteString(")")
		return nil

	case queryplan.NotDistinct:
		return r.writeBinary("IS NOT DISTINCT FROM", t.L, t.R)

	case queryplan.InList:
		r.sb.WriteString("(")
		if err := r.write(t.X); err != nil {
			return err
		}
		r.sb.WriteString(" IN (")
		for i, v := range t.Vs {
			if i > 0 {
				r.sb.WriteString(", ")
			}
			if err := r.write(v); err != nil {
				return err
			}
		}
		r.sb.WriteString("))")
		return nil

	case queryplan.Case:
		r.sb.WriteString("(CASE")
		for _, w := range t.Whens {
			r.sb.WriteString(" WHEN ")
			if err := r.write(w.Cond); err != nil {
				return err
			}
			r.sb.WriteString(" THEN ")
			if err := r.write(w.Then); err != nil {
				return err
			}
		}
		if t.Else != nil {
			r.sb.WriteString(" ELSE ")
			if err := r.write(t.Else); err != nil {
				return err
			}
		}
		r.sb.WriteString(" END)")
		return nil

	case queryplan.Call:
		return r.writeCall(t)

	case queryplan.Cast:
		r.sb.WriteString("CAST(")
		if err := r.write(t.X); err != nil {
			return err
		}
		switch t.To {
		case queryplan.CastText:
			r.sb.WriteString(" AS text)")
		case queryplan.CastFloat:
			r.sb.WriteString(" AS double precision)")
		default:
			r.sb.WriteString(" AS bigint)")
		}
		return nil

	case queryplan.Subquery:
		return r.writeSubquery(t)

	default:
		return fmt.Errorf("cannot render expression of type %T", e)
	}
}

func (r *renderer) writeBinary(symbol string, l, right queryplan.Expr) error {
	r.sb.WriteString("(")
	if err := r.write(l); err != nil {
		return err
	}
	r.sb.WriteString(" ")
	r.sb.WriteString(symbol)
	r.sb.WriteString(" ")
	if err := r.write(right); err != nil {
		return err
	}
	r.sb.WriteString(")")
	return nil
}

func (r *renderer) writeCall(c queryplan.Call) error {
	// `concat` is spelled with `||` rather than the concat() function on purpose: concat()
	// treats NULL as an empty string, which would turn a missing attribute into a match. `||`
	// propagates NULL, keeping the row excluded.
	if c.Name == queryplan.FuncConcat {
		r.sb.WriteString("(")
		for i, arg := range c.Args {
			if i > 0 {
				r.sb.WriteString(" || ")
			}
			if err := r.write(arg); err != nil {
				return err
			}
		}
		r.sb.WriteString(")")
		return nil
	}

	name := map[queryplan.FuncName]string{
		queryplan.FuncCharLength: "char_length",
		queryplan.FuncReplace:    "replace",
		queryplan.FuncNullIf:     "nullif",
	}[c.Name]
	if name == "" {
		return fmt.Errorf("cannot render function %q", c.Name)
	}

	r.sb.WriteString(name)
	r.sb.WriteString("(")
	for i, arg := range c.Args {
		if i > 0 {
			r.sb.WriteString(", ")
		}
		if err := r.write(arg); err != nil {
			return err
		}
	}
	r.sb.WriteString(")")
	return nil
}

func (r *renderer) writeSubquery(s queryplan.Subquery) error {
	if s.Kind == queryplan.SubqueryExists {
		r.sb.WriteString("(EXISTS (SELECT 1 FROM ")
	} else {
		r.sb.WriteString("(SELECT count(*) FROM ")
	}

	for i, item := range s.From {
		if i > 0 {
			r.sb.WriteString(", ")
		}
		r.sb.WriteString(quoteIdent(item.Table))
		r.sb.WriteString(" AS ")
		r.sb.WriteString(quoteIdent(item.Alias))
	}
	r.sb.WriteString(" WHERE ")

	if err := r.write(s.Correlate); err != nil {
		return err
	}
	if s.Where != nil {
		r.sb.WriteString(" AND ")
		if err := r.write(s.Where); err != nil {
			return err
		}
	}

	if s.Kind == queryplan.SubqueryExists {
		r.sb.WriteString("))")
	} else {
		r.sb.WriteString(")")
	}
	return nil
}

func cmpSymbol(op queryplan.CmpOp) string {
	switch op {
	case queryplan.OpEq:
		return "="
	case queryplan.OpNe:
		return "<>"
	case queryplan.OpLt:
		return "<"
	case queryplan.OpLe:
		return "<="
	case queryplan.OpGt:
		return ">"
	default:
		return ">="
	}
}

func arithSymbol(op queryplan.ArithOp) string {
	switch op {
	case queryplan.OpAdd:
		return "+"
	case queryplan.OpSub:
		return "-"
	case queryplan.OpMult:
		return "*"
	case queryplan.OpDiv:
		return "/"
	default:
		return "%"
	}
}
