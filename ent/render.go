// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbosent

import (
	"fmt"
	"time"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"

	"github.com/cerbos/query-plan-adapters/ent/internal/queryplan"
)

// The renderer emits the translated plan through ent's own SQL builder rather than assembling a
// string. Identifier quoting and placeholder syntax differ across the dialects ent supports
// (`?` on MySQL and SQLite, `$n` on PostgreSQL), and `sql.Builder` already applies the right one
// for the dialect the caller's client is configured with — so the adapter never has to know which
// database it is talking to, and a bound value can never be spliced into the SQL text.

// SQLiteTimestampLayout is the textual layout this adapter binds time values in on SQLite.
//
// SQLite has no temporal type, so a timestamp column is text and comparisons are lexicographic.
// That only agrees with chronological order if every value is fixed width and in the same zone,
// which is what this layout guarantees — Go's RFC3339Nano trims trailing zeros from the fraction
// and would order "…:05.5Z" after "…:05.12Z". Callers storing timestamps for SQLite must write
// them in this layout.
const SQLiteTimestampLayout = "2006-01-02T15:04:05.000000000Z"

// render lowers the expression tree into an ent predicate.
//
// A `sql.Predicate` is lazy: its closure runs when the caller's query is built, which is far too
// late for a shape this adapter cannot express — by then the query is already being assembled and
// the failure reads as a malformed statement rather than a refusal to translate. So the tree is
// written once into a throwaway builder first, purely to surface that error from Translate.
func render(e queryplan.Expr, d string) (*sql.Predicate, error) {
	probe := &sql.Builder{}
	probe.SetDialect(d)
	if err := write(probe, e); err != nil {
		return nil, err
	}

	return sql.P(func(b *sql.Builder) {
		if err := write(b, e); err != nil {
			b.AddError(err)
		}
	}), nil
}

// bindValue records a parameter, applying the two dialect-specific adjustments a bound plan value
// needs.
func bindValue(b *sql.Builder, v any) {
	if b.Dialect() == dialect.SQLite {
		// SQLite has no temporal type, so an instant is stored and compared as text.
		if ts, ok := v.(time.Time); ok {
			b.Arg(ts.UTC().Format(SQLiteTimestampLayout))
			return
		}
	}

	b.Arg(v)

	// PostgreSQL infers an untyped `$n` from the context it appears in, and in an expression such
	// as `CAST(col AS double precision) / $1` there is nothing to infer from — it falls back to
	// text and the query dies with "operator does not exist". Since CEL numbers are doubles and
	// the plan is the only thing that knows a literal's type, say it explicitly. SQLite and MySQL
	// infer from the bound value itself and need no annotation.
	if b.Dialect() == dialect.Postgres {
		if t := postgresParamType(v); t != "" {
			b.WriteString("::").WriteString(t)
		}
	}
}

func postgresParamType(v any) string {
	switch v.(type) {
	case bool:
		return "boolean"
	case float32, float64:
		return "double precision"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "bigint"
	case time.Time:
		return "timestamptz"
	case string:
		return "text"
	default:
		return ""
	}
}

// write emits one expression node. A single type switch over the tree is clearer here than
// dispatching through a visitor, and each arm is only a few lines.
//
//nolint:gocyclo // See above: the switch is wide but flat.
func write(b *sql.Builder, e queryplan.Expr) error {
	switch t := e.(type) {
	case queryplan.Column:
		writeColumn(b, t)
		return nil

	case queryplan.Lit:
		if t.V == nil {
			b.WriteString("NULL")
			return nil
		}
		bindValue(b, t.V)
		return nil

	case queryplan.BoolConst:
		// Spelled as a tautology rather than the TRUE/FALSE keywords: MySQL accepts both, but a
		// bare boolean literal is not portable to every dialect ent targets.
		if t.V {
			b.WriteString("1 = 1")
		} else {
			b.WriteString("1 = 0")
		}
		return nil

	case queryplan.Cmp:
		return writeBinary(b, cmpSymbol(t.Op), t.L, t.R)

	case queryplan.Arith:
		return writeBinary(b, arithSymbol(t.Op), t.L, t.R)

	case queryplan.NotDistinct:
		return writeNotDistinct(b, t)

	case queryplan.Logic:
		sep := " OR "
		if t.And {
			sep = " AND "
		}
		return wrap(b, func(b *sql.Builder) error {
			for i, x := range t.Xs {
				if i > 0 {
					b.WriteString(sep)
				}
				if err := write(b, x); err != nil {
					return err
				}
			}
			return nil
		})

	case queryplan.Not:
		return wrap(b, func(b *sql.Builder) error {
			b.WriteString("NOT ")
			return write(b, t.X)
		})

	case queryplan.IsNull:
		return wrap(b, func(b *sql.Builder) error {
			if err := write(b, t.X); err != nil {
				return err
			}
			if t.Negate {
				b.WriteString(" IS NOT NULL")
			} else {
				b.WriteString(" IS NULL")
			}
			return nil
		})

	case queryplan.TruthTest:
		return writeTruthTest(b, t)

	case queryplan.Like:
		return wrap(b, func(b *sql.Builder) error {
			if err := write(b, t.Receiver); err != nil {
				return err
			}
			b.WriteString(" LIKE ")
			if err := write(b, t.Pattern); err != nil {
				return err
			}
			// The escape character is bound rather than inlined so no dialect's string-literal
			// backslash handling can change its meaning.
			b.WriteString(" ESCAPE ")
			b.Arg(`\`)
			return nil
		})

	case queryplan.InList:
		return wrap(b, func(b *sql.Builder) error {
			if err := write(b, t.X); err != nil {
				return err
			}
			b.WriteString(" IN ")
			return wrap(b, func(b *sql.Builder) error {
				for i, v := range t.Vs {
					if i > 0 {
						b.Comma()
					}
					if err := write(b, v); err != nil {
						return err
					}
				}
				return nil
			})
		})

	case queryplan.Case:
		return writeCase(b, t)

	case queryplan.Call:
		return writeCall(b, t)

	case queryplan.Cast:
		return writeCast(b, t)

	case queryplan.Subquery:
		return writeSubquery(b, t)

	default:
		return fmt.Errorf("cannot render expression of type %T", e)
	}
}

// wrap emits a parenthesised group, propagating an error out of the closure.
func wrap(b *sql.Builder, f func(*sql.Builder) error) error {
	var err error
	b.Wrap(func(b *sql.Builder) {
		err = f(b)
	})
	return err
}

func writeColumn(b *sql.Builder, c queryplan.Column) {
	if c.Qualifier != "" {
		b.Ident(c.Qualifier).WriteByte('.')
	}
	b.Ident(c.Name)
}

func writeBinary(b *sql.Builder, symbol string, l, r queryplan.Expr) error {
	return wrap(b, func(b *sql.Builder) error {
		if err := write(b, l); err != nil {
			return err
		}
		b.WriteString(" ").WriteString(symbol).WriteString(" ")
		return write(b, r)
	})
}

// writeNotDistinct emits null-safe equality, using each dialect's own operator.
//
// The obvious portable expansion — `L = R OR (L IS NULL AND R IS NULL)` — is subtly wrong: with
// exactly one side NULL the first disjunct is UNKNOWN and the second is FALSE, so the whole
// expression is UNKNOWN rather than FALSE. That distinction is invisible under a positive test and
// decisive under a negated one, which is exactly what `!(x in coll)` is.
func writeNotDistinct(b *sql.Builder, t queryplan.NotDistinct) error {
	op := "IS NOT DISTINCT FROM"
	switch b.Dialect() {
	case dialect.SQLite:
		op = "IS"
	case dialect.MySQL:
		op = "<=>"
	}
	return writeBinary(b, op, t.L, t.R)
}

// writeTruthTest collapses three-valued logic to two values.
//
// `IS TRUE` / `IS FALSE` are not available on every dialect ent supports, so the test is expanded
// into comparisons that mean the same thing wherever booleans are stored as 0/1 or as a native
// boolean: `x = TRUE`, `NOT (x = TRUE)`, and `x IS NULL`.
func writeTruthTest(b *sql.Builder, t queryplan.TruthTest) error {
	switch t.Want {
	case queryplan.TruthUnknown:
		return write(b, queryplan.IsNull{X: t.X})

	case queryplan.TruthTrue:
		return wrap(b, func(b *sql.Builder) error {
			if err := write(b, t.X); err != nil {
				return err
			}
			b.WriteString(" = ")
			b.Arg(true)
			return nil
		})

	default: // TruthFalse
		return wrap(b, func(b *sql.Builder) error {
			if err := write(b, t.X); err != nil {
				return err
			}
			b.WriteString(" = ")
			b.Arg(false)
			return nil
		})
	}
}

func writeCase(b *sql.Builder, t queryplan.Case) error {
	return wrap(b, func(b *sql.Builder) error {
		b.WriteString("CASE")
		for _, w := range t.Whens {
			b.WriteString(" WHEN ")
			if err := write(b, w.Cond); err != nil {
				return err
			}
			b.WriteString(" THEN ")
			if err := write(b, w.Then); err != nil {
				return err
			}
		}
		if t.Else != nil {
			b.WriteString(" ELSE ")
			if err := write(b, t.Else); err != nil {
				return err
			}
		}
		b.WriteString(" END")
		return nil
	})
}

func writeCall(b *sql.Builder, c queryplan.Call) error {
	// Concatenation must propagate NULL, so that a missing attribute in a dynamic LIKE pattern
	// leaves the match UNKNOWN rather than becoming an empty string that matches.
	//
	// SQLite and PostgreSQL spell that `||`. MySQL does NOT: outside PIPES_AS_CONCAT mode `||`
	// is logical OR, so the pattern would collapse to a boolean and the match would silently
	// fail. Its CONCAT() is NULL-propagating (unlike PostgreSQL's, which skips NULLs), so it is
	// the right spelling there and only there.
	if c.Name == queryplan.FuncConcat {
		return writeConcat(b, c.Args)
	}

	// CEL's size() counts Unicode code points. SQLite's and PostgreSQL's length() do too, but
	// MySQL's LENGTH() counts bytes — "héllo🚀" is 6 to CEL and 10 to MySQL — so it needs
	// CHAR_LENGTH instead.
	charLength := "length"
	if b.Dialect() == dialect.MySQL {
		charLength = "char_length"
	}

	name := map[queryplan.FuncName]string{
		queryplan.FuncCharLength: charLength,
		queryplan.FuncReplace:    "replace",
		queryplan.FuncNullIf:     "nullif",
	}[c.Name]
	if name == "" {
		return fmt.Errorf("cannot render function %q", c.Name)
	}

	b.WriteString(name)
	return wrap(b, func(b *sql.Builder) error {
		for i, arg := range c.Args {
			if i > 0 {
				b.Comma()
			}
			if err := write(b, arg); err != nil {
				return err
			}
		}
		return nil
	})
}

// writeConcat joins strings, propagating NULL.
func writeConcat(b *sql.Builder, args []queryplan.Expr) error {
	if b.Dialect() == dialect.MySQL {
		b.WriteString("CONCAT")
		return wrap(b, func(b *sql.Builder) error {
			return writeSeparated(b, args, ", ")
		})
	}
	return wrap(b, func(b *sql.Builder) error {
		return writeSeparated(b, args, " || ")
	})
}

func writeSeparated(b *sql.Builder, args []queryplan.Expr, separator string) error {
	for i, arg := range args {
		if i > 0 {
			b.WriteString(separator)
		}
		if err := write(b, arg); err != nil {
			return err
		}
	}
	return nil
}

func writeCast(b *sql.Builder, t queryplan.Cast) error {
	// CEL's int() truncates toward zero. SQLite's CAST does the same, but PostgreSQL and MySQL
	// round to nearest — `int(1.9)` is 1 in CEL and 2 there — so truncate explicitly first.
	if t.To == queryplan.CastInt && b.Dialect() != dialect.SQLite {
		if b.Dialect() == dialect.MySQL {
			b.WriteString("CAST(TRUNCATE(")
			if err := write(b, t.X); err != nil {
				return err
			}
			b.WriteString(", 0) AS ").WriteString(castType(b.Dialect(), t.To)).WriteString(")")
			return nil
		}

		b.WriteString("CAST(trunc(")
		if err := write(b, t.X); err != nil {
			return err
		}
		b.WriteString(") AS ").WriteString(castType(b.Dialect(), t.To)).WriteString(")")
		return nil
	}

	b.WriteString("CAST")
	return wrap(b, func(b *sql.Builder) error {
		if err := write(b, t.X); err != nil {
			return err
		}
		b.WriteString(" AS ").WriteString(castType(b.Dialect(), t.To))
		return nil
	})
}

func writeSubquery(b *sql.Builder, s queryplan.Subquery) error {
	return wrap(b, func(b *sql.Builder) error {
		if s.Kind == queryplan.SubqueryExists {
			b.WriteString("EXISTS ")
		}

		return wrap(b, func(b *sql.Builder) error {
			if s.Kind == queryplan.SubqueryExists {
				b.WriteString("SELECT 1 FROM ")
			} else {
				b.WriteString("SELECT COUNT(*) FROM ")
			}

			for i, item := range s.From {
				if i > 0 {
					b.Comma()
				}
				b.Ident(item.Table).WriteString(" AS ").Ident(item.Alias)
			}

			b.WriteString(" WHERE ")
			if err := write(b, s.Correlate); err != nil {
				return err
			}
			if s.Where != nil {
				b.WriteString(" AND ")
				if err := write(b, s.Where); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

// castType spells a CEL conversion for the dialect in use. The three engines ent targets disagree
// on every one of these: MySQL has no `bigint`/`double precision` in CAST, PostgreSQL has no
// `signed`, and SQLite's `real` is its only floating type. Getting the float cast wrong is not
// cosmetic — PostgreSQL's `real` is single precision and would silently round a CEL double.
func castType(d string, to queryplan.CastType) string {
	switch to {
	case queryplan.CastText:
		if d == dialect.MySQL {
			return "char"
		}
		return "text"

	case queryplan.CastFloat:
		switch d {
		case dialect.MySQL:
			return "double"
		case dialect.SQLite:
			return "real"
		default:
			return "double precision"
		}

	default:
		switch d {
		case dialect.MySQL:
			return "signed"
		case dialect.SQLite:
			return "integer"
		default:
			return "bigint"
		}
	}
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
