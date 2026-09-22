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

// str writes SQL text verbatim. Only renderer-owned syntax and quoted identifiers go through it;
// plan values go through bind.
func (r *renderer) str(parts ...string) {
	for _, p := range parts {
		r.sb.WriteString(p)
	}
}

// paren writes body inside a pair of parentheses.
func (r *renderer) paren(body func() error) error {
	r.str("(")
	if err := body(); err != nil {
		return err
	}
	r.str(")")
	return nil
}

// list writes each expression in turn, separated by sep.
func (r *renderer) list(xs []queryplan.Expr, sep string) error {
	for i, x := range xs {
		if i > 0 {
			r.str(sep)
		}
		if err := r.write(x); err != nil {
			return err
		}
	}
	return nil
}

func (r *renderer) writeColumn(c queryplan.Column) {
	if c.Qualifier != "" {
		r.str(quoteIdent(c.Qualifier), ".")
	}
	r.str(quoteIdent(c.Name))
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
			r.str("NULL")
			return nil
		}
		r.bind(t.V)
		return nil

	case queryplan.BoolConst:
		if t.V {
			r.str("TRUE")
		} else {
			r.str("FALSE")
		}
		return nil

	case queryplan.Cmp:
		op, err := symbol(cmpSymbols, "comparison", t.Op)
		if err != nil {
			return err
		}
		return r.writeBinary(op, t.L, t.R)

	case queryplan.Arith:
		op, err := symbol(arithSymbols, "arithmetic", t.Op)
		if err != nil {
			return err
		}
		return r.writeBinary(op, t.L, t.R)

	case queryplan.Concat:
		// PostgreSQL spells string concatenation `||`, which propagates NULL — so a row whose
		// operand is a missing attribute stays excluded under either polarity, matching CEL's
		// error. `+` would be a hard `operator does not exist: text + text` here rather than a
		// silently wrong filter, but the corpus only proves that once the node exists.
		return r.writeBinary("||", t.L, t.R)

	case queryplan.Logic:
		sep := " OR "
		if t.And {
			sep = " AND "
		}
		return r.paren(func() error { return r.list(t.Xs, sep) })

	case queryplan.Not:
		return r.paren(func() error {
			r.str("NOT ")
			return r.write(t.X)
		})

	case queryplan.IsNull:
		test := " IS NULL"
		if t.Negate {
			test = " IS NOT NULL"
		}
		return r.writePostfix(t.X, test)

	case queryplan.TruthTest:
		test, ok := truthTests[t.Want]
		if !ok {
			return fmt.Errorf("cannot render truth value %d", t.Want)
		}
		return r.writePostfix(t.X, test)

	case queryplan.Like:
		return r.paren(func() error {
			if err := r.write(t.Receiver); err != nil {
				return err
			}
			r.str(" LIKE ")
			if err := r.write(t.Pattern); err != nil {
				return err
			}
			// The escape character is bound rather than inlined so that neither
			// standard_conforming_strings nor backslash_quote can change its meaning.
			r.str(" ESCAPE ")
			r.bind(`\`)
			return nil
		})

	case queryplan.NotDistinct:
		return r.writeBinary("IS NOT DISTINCT FROM", t.L, t.R)

	case queryplan.InList:
		return r.paren(func() error {
			if err := r.write(t.X); err != nil {
				return err
			}
			r.str(" IN ")
			return r.paren(func() error { return r.list(t.Vs, ", ") })
		})

	case queryplan.Case:
		return r.writeCase(t)

	case queryplan.Call:
		return r.writeCall(t)

	case queryplan.Cast:
		return r.writeCast(t)

	case queryplan.Subquery:
		return r.writeSubquery(t)

	default:
		return fmt.Errorf("cannot render expression of type %T", e)
	}
}

// truthTests spells each TruthTest state. PostgreSQL has the three-valued tests natively.
var truthTests = map[queryplan.TruthValue]string{
	queryplan.TruthTrue:    " IS TRUE",
	queryplan.TruthFalse:   " IS FALSE",
	queryplan.TruthUnknown: " IS NULL",
}

func (r *renderer) writeBinary(symbol string, left, right queryplan.Expr) error {
	return r.paren(func() error {
		if err := r.write(left); err != nil {
			return err
		}
		r.str(" ", symbol, " ")
		return r.write(right)
	})
}

// writePostfix writes `(x<test>)`, for the IS [NOT] NULL / TRUE / FALSE family.
func (r *renderer) writePostfix(x queryplan.Expr, test string) error {
	return r.paren(func() error {
		if err := r.write(x); err != nil {
			return err
		}
		r.str(test)
		return nil
	})
}

func (r *renderer) writeCase(c queryplan.Case) error {
	return r.paren(func() error {
		r.str("CASE")
		for _, w := range c.Whens {
			r.str(" WHEN ")
			if err := r.write(w.Cond); err != nil {
				return err
			}
			r.str(" THEN ")
			if err := r.write(w.Then); err != nil {
				return err
			}
		}
		if c.Else != nil {
			r.str(" ELSE ")
			if err := r.write(c.Else); err != nil {
				return err
			}
		}
		r.str(" END")
		return nil
	})
}

// writeCast lowers a CEL type conversion.
//
// Only the two casts the translator emits have a spelling; see the CastType constants for why there
// is no integer target. A target this does not know is an error rather than a nearest guess: an
// unspelled cast that fell through to a float would compare against a value the policy never named,
// which is the class of quiet over-grant this adapter refuses on principle.
func (r *renderer) writeCast(c queryplan.Cast) error {
	var target string
	switch c.To {
	case queryplan.CastText:
		target = "text"
	case queryplan.CastFloat:
		target = "double precision"
	default:
		return fmt.Errorf("cannot render cast to %q", c.To)
	}

	r.str("CAST")
	return r.paren(func() error {
		if err := r.write(c.X); err != nil {
			return err
		}
		r.str(" AS ", target)
		return nil
	})
}

func (r *renderer) writeCall(c queryplan.Call) error {
	// `concat` is spelled with `||` rather than the concat() function on purpose: concat()
	// treats NULL as an empty string, which would turn a missing attribute into a match. `||`
	// propagates NULL, keeping the row excluded.
	if c.Name == queryplan.FuncConcat {
		return r.paren(func() error { return r.list(c.Args, " || ") })
	}

	name := functionNames[c.Name]
	if name == "" {
		return fmt.Errorf("cannot render function %q", c.Name)
	}

	r.str(name)
	return r.paren(func() error { return r.list(c.Args, ", ") })
}

func (r *renderer) writeSubquery(s queryplan.Subquery) error {
	body := func() error {
		switch s.Kind {
		case queryplan.SubqueryExists:
			r.str("SELECT 1")
		case queryplan.SubqueryScalar:
			// The projection of a to-one hop. No row correlates -> SQL NULL, which is the
			// missing-attribute error the check side raises (#375).
			r.str("SELECT ")
			if err := r.write(s.Select); err != nil {
				return err
			}
		case queryplan.SubqueryCount:
			r.str("SELECT count(*)")
		default:
			return fmt.Errorf("cannot render subquery kind %d", s.Kind)
		}

		r.str(" FROM ")
		for i, item := range s.From {
			if i > 0 {
				r.str(", ")
			}
			r.str(quoteIdent(item.Table), " AS ", quoteIdent(item.Alias))
		}

		r.str(" WHERE ")
		if err := r.write(s.Correlate); err != nil {
			return err
		}
		if s.Where != nil {
			r.str(" AND ")
			return r.write(s.Where)
		}
		return nil
	}

	if s.Kind == queryplan.SubqueryExists {
		return r.paren(func() error {
			r.str("EXISTS ")
			return r.paren(body)
		})
	}
	return r.paren(body)
}

var cmpSymbols = map[queryplan.CmpOp]string{
	queryplan.OpEq: "=",
	queryplan.OpNe: "<>",
	queryplan.OpLt: "<",
	queryplan.OpLe: "<=",
	queryplan.OpGt: ">",
	queryplan.OpGe: ">=",
}

var arithSymbols = map[queryplan.ArithOp]string{
	queryplan.OpAdd:  "+",
	queryplan.OpSub:  "-",
	queryplan.OpMult: "*",
	queryplan.OpDiv:  "/",
	queryplan.OpMod:  "%",
}

// symbol spells an operator through its table, refusing one the table does not know rather than
// guessing: a wrong operator is valid SQL that quietly returns a different row set.
func symbol[Op ~string](symbols map[Op]string, kind string, op Op) (string, error) {
	s, ok := symbols[op]
	if !ok {
		return "", fmt.Errorf("cannot render %s %q", kind, op)
	}
	return s, nil
}

var functionNames = map[queryplan.FuncName]string{
	queryplan.FuncCharLength: "char_length",
	queryplan.FuncReplace:    "replace",
	queryplan.FuncNullIf:     "nullif",
}
