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

	return predicateFor(e), nil
}

// predicateFor wraps the tree in the lazy predicate ent hands to a selector.
//
// A failure on this pass can only be reported by recording it on the builder — the callback returns
// nothing — and it has to be recorded on the builder the callback was *given*: sql.Builder.Wrap
// hands nested callbacks a fresh builder and copies back only its string and args, so an error
// added there is dropped. write therefore carries errors out through wrap's return value rather than
// calling AddError itself. See TestRenderErrorsEscapeNestedParentheses.
func predicateFor(e queryplan.Expr) *sql.Predicate {
	return sql.P(func(b *sql.Builder) {
		if err := write(b, e); err != nil {
			b.AddError(err)
		}
	})
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

	if _, ok := v.(string); ok && b.Dialect() == dialect.MySQL {
		b.WriteString("CONVERT(")
		b.Arg(v)
		b.WriteString(" USING utf8mb4) COLLATE " + mysqlExactCollation)
		return
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
			b.WriteString("(1 = 1)")
		} else {
			b.WriteString("(1 = 0)")
		}
		return nil

	case queryplan.Cmp:
		op, err := symbol(cmpSymbols, "comparison", t.Op)
		if err != nil {
			return err
		}
		if b.Dialect() == dialect.Postgres && t.Op != queryplan.OpEq && t.Op != queryplan.OpNe &&
			(isText(t.L) || isText(t.R)) {
			return wrap(b, func(b *sql.Builder) error {
				if err := writeCollated(b, t.L, `"C"`); err != nil {
					return err
				}
				b.WriteString(" ").WriteString(op).WriteString(" ")
				return writeCollated(b, t.R, `"C"`)
			})
		}
		return writeBinary(b, op, t.L, t.R)

	case queryplan.Arith:
		op, err := symbol(arithSymbols, "arithmetic", t.Op)
		if err != nil {
			return err
		}
		return writeBinary(b, op, t.L, t.R)

	case queryplan.Concat:
		// The same dialect-correct spelling the escaping path already uses: CONCAT() on MySQL,
		// where `||` is logical OR, and `||` elsewhere. Both propagate NULL here, which is what
		// keeps a row whose operand is a missing attribute excluded under either polarity.
		return writeConcat(b, []queryplan.Expr{t.L, t.R})

	case queryplan.NotDistinct:
		return writeNotDistinct(b, t)

	case queryplan.Logic:
		sep := " OR "
		if t.And {
			sep = " AND "
		}
		return wrap(b, func(b *sql.Builder) error {
			return writeSeparated(b, t.Xs, sep)
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
		if b.Dialect() == dialect.SQLite {
			return writeSQLiteLike(b, t)
		}
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
				return writeSeparated(b, t.Vs, ", ")
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

// mysqlExactCollation is byte-exact and NO PAD, as CEL's string comparison is. MySQL's default,
// utf8mb4_0900_ai_ci, folds case and accents, so every string literal and declared string column
// is compared under it; an explicit COLLATE on either operand decides the comparison.
const mysqlExactCollation = "utf8mb4_0900_bin"

func writeColumn(b *sql.Builder, c queryplan.Column) {
	collate := c.Type == queryplan.ValueString && b.Dialect() == dialect.MySQL
	if collate {
		b.WriteString("(")
	}
	if c.Qualifier != "" {
		b.Ident(c.Qualifier).WriteByte('.')
	}
	b.Ident(c.Name)
	if collate {
		b.WriteString(" COLLATE " + mysqlExactCollation + ")")
	}
}

// isText reports whether e is known to be a string, so a PostgreSQL ordering over it can be
// read under "C": a linguistic collation does not order strings by code point, as CEL does.
func isText(e queryplan.Expr) bool {
	switch t := e.(type) {
	case queryplan.Lit:
		_, ok := t.V.(string)
		return ok
	case queryplan.Column:
		return t.Type == queryplan.ValueString
	case queryplan.Concat:
		return true
	case queryplan.Cast:
		return t.To == queryplan.CastText
	case queryplan.Call:
		return t.Name == queryplan.FuncConcat || t.Name == queryplan.FuncReplace
	}
	return false
}

func writeCollated(b *sql.Builder, e queryplan.Expr, collation string) error {
	if !isText(e) {
		return write(b, e)
	}
	return wrap(b, func(b *sql.Builder) error {
		if err := write(b, e); err != nil {
			return err
		}
		b.WriteString(" COLLATE ").WriteString(collation)
		return nil
	})
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
// boolean: `x = TRUE`, `x = FALSE`, and `x IS NULL`.
func writeTruthTest(b *sql.Builder, t queryplan.TruthTest) error {
	switch t.Want {
	case queryplan.TruthUnknown:
		return write(b, queryplan.IsNull{X: t.X})

	case queryplan.TruthTrue, queryplan.TruthFalse:
		return wrap(b, func(b *sql.Builder) error {
			if err := write(b, t.X); err != nil {
				return err
			}
			b.WriteString(" = ")
			b.Arg(t.Want == queryplan.TruthTrue)
			return nil
		})

	default:
		return fmt.Errorf("cannot render truth value %d", t.Want)
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
	// CHAR_LENGTH instead. SQLite's stops at the first NUL, and its replace() cannot match one, so
	// each NUL becomes a space inside the string's JSON text, where it is the escape \u0000.
	if c.Name == queryplan.FuncCharLength && b.Dialect() == dialect.SQLite {
		b.WriteString(`length(json_extract(replace(replace(replace(json_quote(`)
		if err := write(b, c.Args[0]); err != nil {
			return err
		}
		b.WriteString(`), '\\', char(1)), '\u0000', ' '), char(1), '\\'), '$'))`)
		return nil
	}

	name := functionNames[c.Name]
	if c.Name == queryplan.FuncCharLength && b.Dialect() == dialect.MySQL {
		name = "char_length"
	}
	if name == "" {
		return fmt.Errorf("cannot render function %q", c.Name)
	}

	b.WriteString(name)
	return wrap(b, func(b *sql.Builder) error {
		return writeSeparated(b, c.Args, ", ")
	})
}

// writeSQLiteLike matches a Like's needle without LIKE, which SQLite reads only up to a string's
// first NUL: instr() and = compare every byte, and hex() keeps a suffix aligned to whole bytes.
// Each is case-sensitive, as CEL is, whatever case_sensitive_like says, and propagates NULL.
func writeSQLiteLike(b *sql.Builder, t queryplan.Like) error {
	return wrap(b, func(b *sql.Builder) error {
		operands := func(open, sep, end string) error {
			b.WriteString(open)
			if err := write(b, t.Receiver); err != nil {
				return err
			}
			b.WriteString(sep)
			if err := write(b, t.Needle); err != nil {
				return err
			}
			b.WriteString(end)
			return nil
		}
		switch t.Anchor {
		case queryplan.LikeContains:
			return operands("instr(", ", ", ") > 0")
		case queryplan.LikeStartsWith:
			return operands("instr(", ", ", ") = 1")
		case queryplan.LikeEndsWith:
			// hex() reads NULL as '', so NULL is propagated by hand.
			if err := operands("CASE WHEN ", " IS NOT NULL AND ", " IS NOT NULL THEN "); err != nil {
				return err
			}
			return operands("hex(", ") LIKE '%' || hex(", ") END")
		default:
			return operands("", " = ", "")
		}
	})
}

// writeConcat joins strings, propagating NULL.
func writeConcat(b *sql.Builder, args []queryplan.Expr) error {
	separator := " || "
	if b.Dialect() == dialect.MySQL {
		b.WriteString("CONCAT")
		separator = ", "
	}
	return wrap(b, func(b *sql.Builder) error {
		return writeSeparated(b, args, separator)
	})
}

// writeSeparated writes each expression in turn, separated by separator.
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
	target, err := castType(b.Dialect(), t.To)
	if err != nil {
		return err
	}

	b.WriteString("CAST")
	if err := wrap(b, func(b *sql.Builder) error {
		if err := write(b, t.X); err != nil {
			return err
		}
		b.WriteString(" AS ").WriteString(target)
		return nil
	}); err != nil {
		return err
	}
	if b.Dialect() == dialect.MySQL && t.To == queryplan.CastText {
		// CHAR inherits the connection collation, even when the source column is case-sensitive.
		// Keep character semantics (including size()) and compare without case folding or padding.
		b.WriteString(" COLLATE utf8mb4_0900_bin")
	}
	return nil
}

func writeSubquery(b *sql.Builder, s queryplan.Subquery) error {
	return wrap(b, func(b *sql.Builder) error {
		if s.Kind == queryplan.SubqueryExists {
			b.WriteString("EXISTS ")
		}

		return wrap(b, func(b *sql.Builder) error {
			switch s.Kind {
			case queryplan.SubqueryExists:
				b.WriteString("SELECT 1 FROM ")
			case queryplan.SubqueryScalar:
				// The projection of a to-one hop. No row correlates -> SQL NULL, which is the
				// missing-attribute error the check side raises (#375).
				b.WriteString("SELECT ")
				if err := write(b, s.Select); err != nil {
					return err
				}
				b.WriteString(" FROM ")
			case queryplan.SubqueryCount:
				b.WriteString("SELECT COUNT(*) FROM ")
			default:
				return fmt.Errorf("cannot render subquery kind %d", s.Kind)
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
// on both of these: MySQL spells the text target `char` and has no `double precision`, while
// SQLite's `real` is its only floating type. Getting the float cast wrong is not cosmetic —
// PostgreSQL's `real` is single precision and would silently round a CEL double.
//
// Only the two casts the translator emits have a spelling; see the CastType constants for why there
// is no integer target. A target this does not know is an error rather than a nearest guess: an
// unspelled cast that fell through to a float would compare against a value the policy never named,
// which is the class of quiet over-grant this adapter refuses on principle.
func castType(d string, to queryplan.CastType) (string, error) {
	switch to {
	case queryplan.CastText:
		if d == dialect.MySQL {
			return "char character set utf8mb4", nil
		}
		return "text", nil

	case queryplan.CastFloat:
		switch d {
		case dialect.MySQL:
			return "double", nil
		case dialect.SQLite:
			return "real", nil
		default:
			return "double precision", nil
		}

	default:
		return "", fmt.Errorf("cannot render cast to %q", to)
	}
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
	queryplan.FuncCharLength: "length",
	queryplan.FuncReplace:    "replace",
	queryplan.FuncNullIf:     "nullif",
}
