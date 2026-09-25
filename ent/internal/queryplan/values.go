// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package queryplan

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// A resolved operand is either a plain Go constant folded out of the plan (float64, string, bool,
// nil, []any), an Expr, or a symbolic form. Keeping constants unlifted lets
// whole comparisons fold at translation time — `"const".contains("other")` never reaches SQL.
type value = any

// symbolicValue marks operands that must be consumed before lowering to SQL parameters.
// Keep isSymbolic separate: it identifies only values that need non-finite folding.
type symbolicValue interface {
	isSymbolicValue()
}

func (ieeeConst) isSymbolicValue()          {}
func (condValue) isSymbolicValue()          {}
func (hierarchyValue) isSymbolicValue()     {}
func (deferredCollection) isSymbolicValue() {}

// ieeeConst is a non-finite CEL double. It is deliberately NOT lowered into SQL: no portable SQL
// literal denotes NaN or an infinity, and PostgreSQL's NaN ordering is not IEEE's. Comparisons
// fold it away instead (see compareLeaf).
//
// signUnknown marks an infinity whose sign rests on the sign of a zero column: the value is v or
// -v, and only a result both signs agree on may be folded.
type ieeeConst struct {
	v           float64
	signUnknown bool
}

// errUnknownZeroSign refuses a result that the sign of a stored zero decides. CEL divides by -0.0
// into the opposite infinity from 0.0, and SQL cannot read that sign: no portable function exposes
// the sign bit, and SQLite does not even keep it, storing -0.0 as 0.
var errUnknownZeroSign = errors.New(
	"the sign of a zero column denominator decides this result: CEL's x / -0.0 is the opposite " +
		"infinity from x / 0.0, and SQL cannot read the sign of a stored zero",
)

// foldIEEE applies f to a non-finite constant. An unknown sign survives only as a pair of opposite
// infinities; any other result the sign decides is refused.
func foldIEEE(c ieeeConst, f func(float64) float64) (value, error) {
	out := f(c.v)
	if !c.signUnknown {
		return ieeeConst{v: out}, nil
	}
	alt := f(-c.v)
	switch {
	case math.IsNaN(out) && math.IsNaN(alt), math.Float64bits(out) == math.Float64bits(alt):
		return ieeeConst{v: out}, nil
	case math.IsInf(out, 0) && alt == -out:
		return ieeeConst{v: out, signUnknown: true}, nil
	}
	return nil, errUnknownZeroSign
}

// condValue is a ternary held back from lowering so that a non-finite arm can be folded by the
// enclosing comparison rather than bound as a parameter.
type condValue struct {
	cond Expr
	then value
	els  value
}

// mapArms applies f to both arms, keeping the condition.
func (cv condValue) mapArms(f func(value) (value, error)) (condValue, error) {
	then, err := f(cv.then)
	if err != nil {
		return condValue{}, err
	}
	els, err := f(cv.els)
	if err != nil {
		return condValue{}, err
	}
	return condValue{cond: cv.cond, then: then, els: els}, nil
}

const likeEscape = `\`

// escapeLikeLiteral escapes LIKE metacharacters in a constant needle.
//
// `[` is escaped alongside the portable `%` and `_` because T-SQL treats `[...]` as a character
// class even under an ESCAPE clause. On PostgreSQL and SQLite `\[` with ESCAPE is a literal `[`,
// so escaping it is a semantic no-op there and a correctness fix on SQL Server.
func escapeLikeLiteral(needle string) string {
	r := strings.NewReplacer(
		likeEscape, likeEscape+likeEscape,
		"%", likeEscape+"%",
		"_", likeEscape+"_",
		"[", likeEscape+"[",
	)
	return r.Replace(needle)
}

// escapeLikeColumn escapes LIKE metacharacters in a column-valued needle at query time.
//
// A NULL needle propagates through REPLACE to a NULL pattern, so the LIKE stays UNKNOWN and the
// row is excluded — which is what CEL's missing-attribute error (a deny) means for that row.
func escapeLikeColumn(needle Expr) Expr {
	replace := func(x Expr, from, to string) Expr {
		return Call{Name: FuncReplace, Args: []Expr{x, Lit{V: from}, Lit{V: to}}}
	}
	out := replace(needle, likeEscape, likeEscape+likeEscape)
	out = replace(out, "%", likeEscape+"%")
	out = replace(out, "_", likeEscape+"_")
	return replace(out, "[", likeEscape+"[")
}

// stringMatch translates CEL contains/startsWith/endsWith into an escaped LIKE.
//
// Operands arrive in CEL source order, receiver first. Both sides may be either a constant or a
// column: `"const".contains(R.attr.x)` puts the constant in the receiver position, and a
// field-to-field match puts a column in the needle. prefix/suffix add `%` before/after the
// escaped needle.
//
// LIKE collation is dialect-controlled while CEL string matching is case-sensitive, so a
// case-insensitive collation over-grants here. That is a documented part of each adapter's
// contract rather than something the translator can fix.
func stringMatch(receiver, needle value, prefix, suffix bool) (Expr, error) {
	if knownNonString(receiver) || knownNonString(needle) {
		// A string operator over a declared non-string is a CEL no-overload error: UNKNOWN.
		return Lit{V: nil}, nil
	}
	recvStr, recvIsStr := receiver.(string)
	needleStr, needleIsStr := needle.(string)

	// Both sides constant: fold in Go rather than emitting SQL that only agrees by accident.
	if recvIsStr && needleIsStr {
		switch {
		case prefix && suffix:
			return BoolConst{V: strings.Contains(recvStr, needleStr)}, nil
		case suffix:
			return BoolConst{V: strings.HasPrefix(recvStr, needleStr)}, nil
		case prefix:
			return BoolConst{V: strings.HasSuffix(recvStr, needleStr)}, nil
		}
	}

	recvExpr, err := asExpr(receiver)
	if err != nil {
		return nil, err
	}

	pattern, err := likePattern(needle, prefix, suffix)
	if err != nil {
		return nil, err
	}

	return Like{Receiver: recvExpr, Pattern: pattern}, nil
}

// likePattern builds the escaped pattern for a needle that is either a constant or a column.
//
// A constant is escaped once at translation time; a column is escaped by the database at query
// time, and its NULL propagates through REPLACE and the concatenation so the LIKE stays UNKNOWN.
func likePattern(needle value, prefix, suffix bool) (Expr, error) {
	if s, ok := needle.(string); ok {
		lit := escapeLikeLiteral(s)
		if prefix {
			lit = "%" + lit
		}
		if suffix {
			lit += "%"
		}
		return Lit{V: lit}, nil
	}

	needleExpr, err := asExpr(needle)
	if err != nil {
		return nil, err
	}

	parts := []Expr{escapeLikeColumn(needleExpr)}
	if prefix {
		parts = append([]Expr{Lit{V: "%"}}, parts...)
	}
	if suffix {
		parts = append(parts, Lit{V: "%"})
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return Call{Name: FuncConcat, Args: parts}, nil
}

// floatDiv implements CEL division.
//
// CEL attribute arithmetic is double-typed (Cerbos transports every number as a double), so
// integer division would truncate `3 / 2.0` to `1` on SQLite and PostgreSQL. More importantly a
// zero denominator is NOT an error in CEL: `0/0` is NaN and `x/0` is a signed infinity. Lowering
// those to SQL NULL loses the distinction — `NULL != 1.0` is UNKNOWN and excludes the row, while
// `NaN != 1.0` is TRUE and the PDP allows it.
//
// The three IEEE cases stay symbolic and the enclosing comparison folds each arm, which is exact
// for ordered and equality comparisons alike. A NULL numerator or denominator makes every branch
// condition UNKNOWN, so the folded CASE yields NULL and the row stays excluded under both
// polarities — the right outcome for a CEL missing-attribute error.
func floatDiv(l, r value) (value, error) {
	ln, lIsNum := asFloat(l)
	rn, rIsNum := asFloat(r)

	if lIsNum && rIsNum {
		if rn == 0 {
			if ln == 0 || math.IsNaN(ln) {
				return ieeeConst{v: math.NaN()}, nil
			}
			return ieeeConst{v: math.Copysign(math.Inf(1), math.Copysign(1, ln)*math.Copysign(1, rn))}, nil
		}
		return ln / rn, nil
	}

	numerator, err := asFloatExpr(l)
	if err != nil {
		return nil, err
	}
	denominator, err := asFloatExpr(r)
	if err != nil {
		return nil, err
	}

	zero := Lit{V: float64(0)}
	// The finite arm keeps a NULLIF guard: it can never be selected when the denominator is
	// zero, but dialects that evaluate CASE arms eagerly would otherwise abort the whole query.
	finite := Arith{
		Op: OpDiv,
		L:  numerator,
		R:  Call{Name: FuncNullIf, Args: []Expr{denominator, zero}},
	}

	// A self-division has no infinite arm: a zero denominator is a zero numerator, so it is NaN.
	if reflect.DeepEqual(numerator, denominator) {
		return condValue{cond: Cmp{Op: OpEq, L: denominator, R: zero}, then: ieeeConst{v: math.NaN()}, els: finite}, nil
	}

	// IEEE-754 keeps the sign of a zero, so `n / -0.0` is the OPPOSITE infinity from `n / 0.0`.
	// A CONSTANT denominator carries its sign on the wire (the planner ships `-0` verbatim and
	// protobuf doubles preserve the sign bit), so it must be applied. A COLUMN denominator does
	// not: SQL cannot tell -0.0 from 0.0 and no portable function reads the sign bit, so the
	// infinity's sign is left unknown and a comparison it decides is refused (errUnknownZeroSign).
	denominatorSign := 1.0
	if rIsNum && math.Signbit(rn) {
		denominatorSign = -1.0
	}

	return condValue{
		cond: Cmp{Op: OpEq, L: denominator, R: zero},
		then: condValue{
			cond: Cmp{Op: OpEq, L: numerator, R: zero},
			then: ieeeConst{v: math.NaN()},
			els: condValue{
				cond: Cmp{Op: OpGt, L: numerator, R: zero},
				then: ieeeConst{v: math.Inf(int(denominatorSign)), signUnknown: !rIsNum},
				els:  ieeeConst{v: math.Inf(-int(denominatorSign)), signUnknown: !rIsNum},
			},
		},
		els: finite,
	}, nil
}

// errNonFiniteWithColumn is returned when a NaN or infinity would have to be combined with a
// column value: SQL has no literal for either, so there is nothing to bind.
var errNonFiniteWithColumn = errors.New(
	"arithmetic combines a non-finite value with a column, which SQL cannot carry",
)

// arithOverConditional distributes a binary arithmetic operator across a retained ternary, so a
// non-finite arm keeps propagating symbolically instead of being lowered to SQL.
//
// `R.attr.aNumber / R.attr.aNumber + 1.0` composes addition on top of a division that is NaN for
// a zero row. Lowering that arm to SQL turns it into `NULL + 1`, and `NULL != 2.0` is UNKNOWN
// where CEL's `NaN != 2.0` is TRUE — the row the PDP allows would be dropped. Returns (nil, false)
// when neither operand is conditional.
func arithOverConditional(op ArithOp, l, r value) (value, bool, error) {
	combine := func(left, right value) (value, error) {
		lf, lok := asFloat(left)
		rf, rok := asFloat(right)
		if lok && rok {
			folded, err := foldArithmetic(op, lf, rf)
			if err != nil {
				return nil, err
			}
			if folded != nil {
				return *folded, nil
			}
		}
		if op == OpMod {
			// CEL's % is integer-only while Cerbos attribute values are always doubles, so a
			// modulus over this arithmetic is a no-overload error that denies every row at check
			// time. Folding it would answer a question CEL refused.
			return nil, fmt.Errorf(
				"modulus over a division whose denominator may be zero is not supported: CEL's " +
					"%% is integer-only and attribute values are always doubles, so the condition " +
					"can never be satisfied by the PDP",
			)
		}
		if inner, ok, err := arithOverConditional(op, left, right); err != nil || ok {
			return inner, err
		}
		// A non-finite operand absorbs every finite one under +, -, * and /, so fold it here
		// rather than asking asExpr for a SQL representation that does not exist.
		if lc, ok := left.(ieeeConst); ok {
			rf, ok := asFloat(right)
			if !ok {
				return nil, errNonFiniteWithColumn
			}
			return foldIEEE(lc, func(v float64) float64 { return applyIEEE(op, v, rf) })
		}
		if rc, ok := right.(ieeeConst); ok {
			lf, ok := asFloat(left)
			if !ok {
				return nil, errNonFiniteWithColumn
			}
			return foldIEEE(rc, func(v float64) float64 { return applyIEEE(op, lf, v) })
		}
		lExpr, err := asExpr(left)
		if err != nil {
			return nil, err
		}
		rExpr, err := asExpr(right)
		if err != nil {
			return nil, err
		}
		return Arith{Op: op, L: lExpr, R: rExpr}, nil
	}

	if cv, ok := l.(condValue); ok {
		out, err := cv.mapArms(func(arm value) (value, error) { return combine(arm, r) })
		return out, true, err
	}
	if cv, ok := r.(condValue); ok {
		out, err := cv.mapArms(func(arm value) (value, error) { return combine(l, arm) })
		return out, true, err
	}
	return nil, false, nil
}

// applyIEEE evaluates an arithmetic operator in Go's IEEE double space, which is CEL's own.
func applyIEEE(op ArithOp, l, r float64) float64 {
	switch op {
	case OpAdd:
		return l + r
	case OpSub:
		return l - r
	case OpMult:
		return l * r
	case OpDiv:
		return l / r
	case OpMod:
		// arithOverConditional rejects OpMod before substituting an arm: CEL's % is
		// integer-only over doubles, so there is no IEEE answer to give.
	}
	return math.NaN()
}

// compare builds a comparison, distributing over any retained ternary so that a non-finite arm
// reaches compareLeaf as a constant instead of leaking PostgreSQL's non-IEEE NaN ordering.
func compare(op CmpOp, l, r value) (Expr, error) {
	if cv, ok := l.(condValue); ok {
		return distribute(op, cv, r, true)
	}
	if cv, ok := r.(condValue); ok {
		return distribute(op, cv, l, false)
	}
	return compareLeaf(op, l, r)
}

func distribute(op CmpOp, cv condValue, other value, condOnLeft bool) (Expr, error) {
	build := func(arm value) (Expr, error) {
		if condOnLeft {
			return compare(op, arm, other)
		}
		return compare(op, other, arm)
	}

	thenExpr, err := build(cv.then)
	if err != nil {
		return nil, err
	}
	elseExpr, err := build(cv.els)
	if err != nil {
		return nil, err
	}

	// Both arms are guarded rather than using ELSE: an UNKNOWN condition must yield NULL, not
	// the else-branch, so the row stays excluded under both polarities.
	return Case{Whens: []When{
		{Cond: cv.cond, Then: thenExpr},
		{Cond: Not{X: cv.cond}, Then: elseExpr},
	}}, nil
}

func compareLeaf(op CmpOp, l, r value) (Expr, error) {
	lIEEE, lIsIEEE := l.(ieeeConst)
	rIEEE, rIsIEEE := r.(ieeeConst)

	if !lIsIEEE && !rIsIEEE {
		return applyComparison(op, l, r)
	}

	lNaN := lIsIEEE && math.IsNaN(lIEEE.v)
	rNaN := rIsIEEE && math.IsNaN(rIEEE.v)

	if lNaN || rNaN {
		other := r
		if rNaN {
			other = l
		}
		if lNaN && rNaN {
			other = nil
		}

		// Cerbos 0.55 follows IEEE: NaN is unequal to everything; ordered comparisons
		// are false, so negation is true. Missing attributes still propagate UNKNOWN below.
		result := BoolConst{V: op == OpNe}

		if _, ok := asFloat(other); ok || other == nil {
			return result, nil
		}
		if otherExpr, ok := other.(Expr); ok {
			// Preserve a CEL missing-attribute error as SQL UNKNOWN while folding every
			// present numeric value dialect-independently.
			return Case{Whens: []When{
				{Cond: IsNull{X: otherExpr}, Then: Lit{V: nil}},
				{Cond: IsNull{X: otherExpr, Negate: true}, Then: result},
			}}, nil
		}
		return nil, fmt.Errorf("NaN can only be compared with numeric constants or column expressions")
	}

	lv, lok := asFloat(l)
	if lIsIEEE {
		lv, lok = lIEEE.v, true
	}
	rv, rok := asFloat(r)
	if rIsIEEE {
		rv, rok = rIEEE.v, true
	}
	if !lok || !rok {
		return nil, fmt.Errorf("non-finite numeric constants can only be compared with numeric constants")
	}

	result := compareOrdered(op, lv, rv)
	lUnknown := lIsIEEE && lIEEE.signUnknown
	rUnknown := rIsIEEE && rIEEE.signUnknown
	if (lUnknown && compareOrdered(op, -lv, rv) != result) ||
		(rUnknown && compareOrdered(op, lv, -rv) != result) ||
		(lUnknown && rUnknown && compareOrdered(op, -lv, -rv) != result) {
		return nil, errUnknownZeroSign
	}
	return BoolConst{V: result}, nil
}

// compareOrdered folds a comparison between two constants of the same ordered type.
func compareOrdered[T cmp.Ordered](op CmpOp, l, r T) bool {
	switch op {
	case OpEq:
		return l == r
	case OpNe:
		return l != r
	case OpLt:
		return l < r
	case OpLe:
		return l <= r
	case OpGt:
		return l > r
	default:
		return l >= r
	}
}

// applyComparison lowers a comparison whose operands are ordinary constants or expressions.
func applyComparison(op CmpOp, l, r value) (Expr, error) {
	if text, ok, err := compareNumberText(op, l, r); err != nil || ok {
		return text, err
	}
	if mixed, ok := compareMixedTypes(op, l, r); ok {
		return mixed, nil
	}
	if _, list := l.([]any); list {
		return nil, errListOperand
	}
	if _, list := r.([]any); list {
		return nil, errListOperand
	}
	if nullTest, ok, err := nullComparison(op, l, r); err != nil || ok {
		return nullTest, err
	}

	// Both sides constant: fold rather than round-tripping through the database.
	if lf, ok := asFloat(l); ok {
		if rf, ok := asFloat(r); ok {
			return BoolConst{V: compareOrdered(op, lf, rf)}, nil
		}
	}
	if ls, ok := l.(string); ok {
		if rs, ok := r.(string); ok {
			return BoolConst{V: compareOrdered(op, ls, rs)}, nil
		}
	}
	if lb, ok := l.(bool); ok {
		if rb, ok := r.(bool); ok {
			switch op {
			case OpEq:
				return BoolConst{V: lb == rb}, nil
			case OpNe:
				return BoolConst{V: lb != rb}, nil
			default:
				// CEL does not order booleans, so an ordered comparison between two of them is
				// not a shape the planner can emit. Fall through to the expression path rather
				// than inventing an ordering here.
			}
		}
	}

	lExpr, err := asExpr(l)
	if err != nil {
		return nil, err
	}
	rExpr, err := asExpr(r)
	if err != nil {
		return nil, err
	}
	if err := assertNoMixedNullConventions(op, lExpr, rExpr); err != nil {
		return nil, err
	}
	if definite, ok := definiteEquality(op, lExpr, rExpr); ok {
		return definite, nil
	}
	return Cmp{Op: op, L: lExpr, R: rExpr}, nil
}

// errListOperand refuses a list literal compared with an operand whose type is not declared. CEL
// compares a list with a list element by element and answers any other type unequal, and SQL has
// neither a list operand nor a way to tell an undeclared column's type.
var errListOperand = errors.New(
	"a list literal compares only against an operand with a declared scalar ValueType, which it " +
		"never equals; SQL has no list operand to compare an undeclared column with",
)

// numberText is string() over a column declared ValueNumber, held back from lowering.
//
// CEL spells a double with Go's %g — `1e+06`, `2`, `-9.5e+18`, `-0` — and no engine's CAST prints
// that: PostgreSQL and MySQL write `1000000`, SQLite writes `2.0`. Equality against a string
// constant does not need the spelling, though: it becomes a numeric comparison with the one double
// that prints as the constant, or false when none does.
type numberText struct {
	x Expr
}

func (numberText) isSymbolicValue() {}

// compareNumberText lowers `string(number) == "…"` and `!=`. Any other use of numberText is refused.
func compareNumberText(op CmpOp, l, r value) (Expr, bool, error) {
	text, ok := l.(numberText)
	other := r
	if !ok {
		if text, ok = r.(numberText); !ok {
			return nil, false, nil
		}
		other = l
	}
	s, isString := other.(string)
	if !isString || (op != OpEq && op != OpNe) {
		return nil, true, errNumberText
	}
	f, spelled := celDoubleSpelling(s)
	if !spelled {
		// No double prints as s, so the comparison is decided wherever the column is present. A
		// NULL is a CEL error (string() has no null overload) and stays UNKNOWN.
		return Case{Whens: []When{{Cond: IsNull{X: text.x, Negate: true}, Then: BoolConst{V: op == OpNe}}}}, true, nil
	}
	if f == 0 {
		// "0" and "-0" are told apart by the sign bit alone, which SQL cannot read.
		return nil, true, fmt.Errorf(
			"string() over a number compared with %q: CEL spells 0.0 \"0\" and -0.0 \"-0\", and SQL "+
				"cannot read the sign of a stored zero", s,
		)
	}
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return nil, true, fmt.Errorf("string() over a number compared with %q: a non-finite value has no SQL literal", s)
	}
	return Cmp{Op: op, L: text.x, R: Lit{V: f}}, true, nil
}

// celDoubleSpelling returns the double CEL's string() spells as s, if there is one.
func celDoubleSpelling(s string) (float64, bool) {
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil && fmt.Sprintf("%g", f) == s
}

var errNumberText = errors.New(
	"string() over a ValueNumber column translates only as == or != against a string constant: " +
		"CEL spells a double with Go's %g (\"1e+06\", \"-0\") and no SQL CAST prints that spelling",
)

// definiteEquality rewrites an equality involving an explicit-null column so it is never UNKNOWN.
//
// A null VALUE is what CEL holds under that convention, so `null == "x"` is a definite FALSE,
// `null != "x"` a definite TRUE, and two nulls are EQUAL. SQL answers UNKNOWN to all three, which
// excludes the row under BOTH polarities — so the NOT an enclosing negation applies has nothing
// definite to flip. Making the predicate definite here is what lets the rest of the translator go
// on treating NOT as an ordinary SQL NOT.
//
// Deliberately not the NotDistinct node this package already carries for elementMatches, even
// though it renders a null-safe equality on all three dialects. NotDistinct is symmetric, and this
// rewrite must not be: when only ONE side declares the convention, the other side's NULL is a
// MISSING attribute on the check side, so CEL raises an error and denies — and only the asymmetric
// expansion below keeps propagating UNKNOWN for it. A null-safe equality would match the two NULLs
// and over-grant. NotDistinct is right in elementMatches because both operands there are on the
// explicit convention by construction.
func definiteEquality(op CmpOp, l, r Expr) (Expr, bool) {
	if op != OpEq && op != OpNe {
		return nil, false
	}
	lCol, lExplicit := explicitNullColumn(l)
	rCol, rExplicit := explicitNullColumn(r)
	if !lExplicit && !rExplicit {
		return nil, false
	}
	// Mixing the two conventions across one comparison has no faithful rendering, so it is
	// refused by the caller rather than answered here. A Lit operand is different: a constant
	// can never be a missing attribute, so one declared column against a constant is fine.
	if _, lIsColumn := l.(Column); lIsColumn {
		if _, rIsColumn := r.(Column); rIsColumn && (!lExplicit || !rExplicit) {
			return nil, false
		}
	}

	// At most both sides' presence tests plus the equality itself.
	const maxDefiniteEqualityParts = 3
	present := make([]Expr, 0, maxDefiniteEqualityParts)
	if lExplicit {
		present = append(present, IsNull{X: lCol, Negate: true})
	}
	if rExplicit {
		present = append(present, IsNull{X: rCol, Negate: true})
	}
	present = append(present, Cmp{Op: OpEq, L: l, R: r})

	equality := and(present...)
	if lExplicit && rExplicit {
		equality = or(and(IsNull{X: lCol}, IsNull{X: rCol}), equality)
	}
	if op == OpNe {
		return Not{X: equality}, true
	}
	return equality, true
}

// assertNoMixedNullConventions refuses an equality between two columns where only ONE declares
// the explicit-null convention.
//
// The declared side needs a definite answer for its NULL — CEL holds a null VALUE there. The
// undeclared side needs UNKNOWN for its NULL — that is a missing attribute, which CEL denies under
// BOTH polarities. A definite predicate returns rows the PDP refuses; a plain one drops rows the
// PDP allows. Neither is the decision, so the shape is refused rather than answered in a direction.
func assertNoMixedNullConventions(op CmpOp, l, r Expr) error {
	if op != OpEq && op != OpNe {
		return nil
	}
	lCol, lIsColumn := l.(Column)
	rCol, rIsColumn := r.(Column)
	if !lIsColumn || !rIsColumn || lCol.ExplicitNull == rCol.ExplicitNull {
		return nil
	}
	return fmt.Errorf(
		"cannot compare %q and %q under mixed null conventions: one declares the explicit-null "+
			"convention and the other does not, so the omitted side is UNKNOWN for a NULL column "+
			"while the declared side is definite, and no single predicate is both. Declare "+
			"NullConvention on both mapper entries, or on neither",
		lCol.Name, rCol.Name,
	)
}

func explicitNullColumn(e Expr) (Expr, bool) {
	col, ok := e.(Column)
	return col, ok && col.ExplicitNull
}

// membership implements CEL `in`, including explicit-null list elements.
//
// `x in [null, "a"]` must match the rows where x IS NULL as well as those equal to "a", because
// the corpus's explicit-null convention sends a NULL column as a real null attribute. SQL's
// `IN (NULL, 'a')` never matches a NULL row, so the null members become an explicit IS NULL arm.
func membership(x, values value) (Expr, error) {
	if _, list := x.([]any); list {
		return nil, fmt.Errorf("membership with a list-valued element cannot be represented by scalar SQL IN")
	}
	members, ok := values.([]any)
	if !ok {
		members = []any{values}
	}

	xExpr, err := asExpr(x)
	if err != nil {
		return nil, err
	}

	// CEL's equality is heterogeneous: a member whose type differs from x's declared type is
	// unequal to it, where SQL would coerce ('2' onto a numeric column, 'true' onto a boolean
	// one) and match. Such a member can never witness the membership, so it is dropped.
	xKind := scalarKind(x)
	nonNull := make([]Expr, 0, len(members))
	hasNull, dropped := false, false
	for _, m := range members {
		if m == nil {
			hasNull = true
			continue
		}
		if kind := scalarKind(m); xKind != "" && kind != "" && kind != xKind {
			dropped = true
			continue
		}
		nonNull = append(nonNull, Lit{V: m})
	}

	var predicates []Expr
	if len(nonNull) > 0 {
		list := Expr(InList{X: xExpr, Vs: nonNull})
		// Without a null member nothing has made the membership definite yet: `NOT (col IN (…))`
		// over a NULL column is UNKNOWN and drops the row, while CEL compares a null VALUE
		// against each element and gets a definite false. With a null member the IS NULL arm
		// below already settles it.
		if col, explicit := explicitNullColumn(xExpr); explicit && !hasNull {
			list = and(IsNull{X: col, Negate: true}, list)
		}
		predicates = append(predicates, list)
	}
	if hasNull {
		predicates = append(predicates, IsNull{X: xExpr})
	}
	if len(predicates) == 0 {
		if _, explicit := explicitNullColumn(xExpr); dropped && !explicit {
			// Every member had another type: false wherever x is present. A NULL x not on the
			// explicit-null convention is a missing attribute, which CEL denies under both
			// polarities, so it stays UNKNOWN rather than letting a negation turn it into an allow.
			return Case{Whens: []When{{Cond: IsNull{X: xExpr, Negate: true}, Then: BoolConst{V: false}}}}, nil
		}
		// `x in []` is false for every row, and the planner usually folds it to ALWAYS_DENIED.
		return BoolConst{V: false}, nil
	}
	return or(predicates...), nil
}

var (
	rfc3339Timestamp = regexp.MustCompile(
		`^((?:\d{4}))-(\d{2})-(\d{2})[Tt]` +
			`(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d` +
			`(?:\.(\d{1,9}))?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)$`,
	)
	minCELTimestamp = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	maxCELTimestamp = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
)

// parseTimestamp unwraps a temporal column or parses an RFC-3339 planner constant.
//
// Go's time.Time carries nanoseconds, so unlike the Python and TypeScript adapters this does not
// have to reject the planner's sub-millisecond now() literal — the `timestamp/*/relative-window*` cases are
// translatable here.
func parseTimestamp(v value, entryIsTemporal bool) (value, error) {
	if entryIsTemporal {
		return v, nil
	}

	s, ok := v.(string)
	if !ok {
		if _, isExpr := v.(Expr); isExpr {
			return nil, fmt.Errorf(
				"timestamp() over a column requires that column to be mapped with a timestamp value type",
			)
		}
		return nil, fmt.Errorf("timestamp() requires an RFC-3339 literal or a temporal column")
	}

	m := rfc3339Timestamp.FindStringSubmatch(s)
	if m == nil {
		return nil, fmt.Errorf("invalid RFC-3339 timestamp literal: %s", s)
	}
	if m[1] == "0000" {
		return nil, fmt.Errorf("invalid RFC-3339 timestamp literal: %s", s)
	}

	parsed, err := time.Parse(time.RFC3339Nano, strings.ToUpper(s[:len(s)-1])+s[len(s)-1:])
	if err != nil {
		parsed, err = time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return nil, fmt.Errorf("invalid RFC-3339 timestamp literal: %s", s)
		}
	}

	normalized := parsed.UTC()
	if normalized.Before(minCELTimestamp) || normalized.After(maxCELTimestamp) {
		return nil, fmt.Errorf("timestamp literal is outside CEL's supported instant range: %s", s)
	}
	return normalized, nil
}

// hierarchyValue is a hierarchy() call held symbolically until the enclosing hierarchy operator
// consumes it, so the delimiter travels with the value.
type hierarchyValue struct {
	value     value
	delimiter string
}

func newHierarchy(v, delimiter value) (hierarchyValue, error) {
	d := "."
	if delimiter != nil {
		s, ok := delimiter.(string)
		if !ok || s == "" {
			return hierarchyValue{}, fmt.Errorf("hierarchy() delimiter must be a non-empty string")
		}
		d = s
	}
	return hierarchyValue{value: v, delimiter: d}, nil
}

func matchingHierarchies(l, r value) (hierarchyValue, hierarchyValue, error) {
	lh, lok := l.(hierarchyValue)
	rh, rok := r.(hierarchyValue)
	if !lok || !rok {
		return hierarchyValue{}, hierarchyValue{}, fmt.Errorf("hierarchy operator requires hierarchy() operands")
	}
	if lh.delimiter != rh.delimiter {
		return hierarchyValue{}, hierarchyValue{}, fmt.Errorf("hierarchy operands must use the same delimiter")
	}
	return lh, rh, nil
}

// ancestorOf is true when `ancestor` is a strict prefix of `descendent` at a delimiter boundary.
func ancestorOf(l, r value) (Expr, error) {
	ancestor, descendent, err := matchingHierarchies(l, r)
	if err != nil {
		return nil, err
	}

	aStr, aIsStr := ancestor.value.(string)
	dStr, dIsStr := descendent.value.(string)
	delim := ancestor.delimiter

	switch {
	case aIsStr && dIsStr:
		return BoolConst{V: strings.HasPrefix(dStr, aStr+delim)}, nil

	case dIsStr:
		// The descendent is constant, so enumerate its proper prefixes and test membership.
		// This is exact and avoids a LIKE against a column-valued pattern.
		parts := strings.Split(dStr, delim)
		prefixes := make([]any, 0, len(parts))
		for i := 1; i < len(parts); i++ {
			prefixes = append(prefixes, strings.Join(parts[:i], delim))
		}
		return membership(ancestor.value, prefixes)

	case aIsStr:
		return stringMatch(descendent.value, aStr+delim, false, true)

	default:
		return nil, fmt.Errorf("hierarchy comparison between two columns is not supported")
	}
}

func descendentOf(l, r value) (Expr, error) {
	return ancestorOf(r, l)
}

// hierarchyOverlaps is true when either side is an ancestor of the other, or they are equal.
func hierarchyOverlaps(l, r value) (Expr, error) {
	lh, rh, err := matchingHierarchies(l, r)
	if err != nil {
		return nil, err
	}

	eq, err := compare(OpEq, lh.value, rh.value)
	if err != nil {
		return nil, err
	}
	lAnc, err := ancestorOf(lh, rh)
	if err != nil {
		return nil, err
	}
	rAnc, err := ancestorOf(rh, lh)
	if err != nil {
		return nil, err
	}
	return or(eq, lAnc, rAnc), nil
}

// asExpr lifts a resolved operand into the expression tree.
func asExpr(v value) (Expr, error) {
	switch t := v.(type) {
	case nil:
		return Lit{V: nil}, nil
	case Expr:
		return t, nil
	case ieeeConst:
		return nil, fmt.Errorf("non-finite numeric value has no SQL representation")
	case condValue:
		return nil, fmt.Errorf("conditional value used where a plain expression is required")
	case hierarchyValue:
		return nil, fmt.Errorf("hierarchy() value used outside a hierarchy operator")
	case numberText:
		return nil, errNumberText
	case deferredCollection:
		// A filter()/map() reaching a plain VALUE position: `map(tags, t.id) == [...]` compares
		// the projection itself rather than feeding it to size() or hasIntersection(). Without
		// this case the default arm bound the held collection as a query PARAMETER, so the
		// translator emitted a filter for a shape it cannot express and only the driver's
		// encoder refused it, at execution time (cerbos/query-plan-adapters#387).
		return nil, fmt.Errorf("'%s' produces a collection rather than a plain value; it only translates inside size() or hasIntersection(), which give the collection a scalar meaning", t.macro())
	case symbolicValue:
		return nil, fmt.Errorf("symbolic value %T has no SQL representation", v)
	default:
		return Lit{V: v}, nil
	}
}

// asFloat reports whether v is a numeric constant. Every number the plan carries decodes to
// float64 (see decodeValue), and every constant the translator folds stays one. Booleans are
// excluded: CEL does not treat them as numbers.
func asFloat(v value) (float64, bool) {
	f, ok := v.(float64)
	return f, ok
}

// asFloatExpr lifts an operand to a float-typed expression, casting a column so that integer
// division cannot truncate.
func asFloatExpr(v value) (Expr, error) {
	if f, ok := asFloat(v); ok {
		return Lit{V: f}, nil
	}
	e, err := asExpr(v)
	if err != nil {
		return nil, err
	}
	return Cast{X: e, To: CastFloat}, nil
}

// omittedNullComparison lowers `x == null` / `x != null` for an attribute declaring
// NullConventionOmitted.
//
// A NULL column sends no attribute, so CEL raises a missing-attribute error for it, and a present
// column is never null: `==` is false and `!=` is true. The CASE yields exactly that, with SQL
// NULL standing for the error. UNKNOWN stays UNKNOWN under NOT, so the rendering is right under
// any nesting without tracking negation parity, and an enclosing OR still absorbs it when a
// sibling is true, as CEL's `||` absorbs the error. A column read through a to-ONE hop is NULL
// for an absent parent too, which is the same missing-path error.
func omittedNullComparison(op CmpOp, x Expr) Expr {
	return Case{
		Whens: []When{{Cond: IsNull{X: x}, Then: Lit{V: nil}}},
		Else:  BoolConst{V: op == OpNe},
	}
}

// nullComparison lowers `x == null` / `x != null` into a NULL test.
//
// `x = NULL` is UNKNOWN for every row in SQL, whereas CEL's `x == null` is true exactly for the
// null-valued rows, so the comparison has to become IS NULL. The second return value reports
// whether this was a null comparison at all.
func nullComparison(op CmpOp, l, r value) (Expr, bool, error) {
	if op != OpEq && op != OpNe {
		return nil, false, nil
	}
	if l != nil && r != nil {
		return nil, false, nil
	}

	other := r
	if r == nil {
		other = l
	}
	if other == nil {
		return BoolConst{V: op == OpEq}, true, nil
	}

	otherExpr, err := asExpr(other)
	if err != nil {
		return nil, false, err
	}
	return IsNull{X: otherExpr, Negate: op == OpNe}, true, nil
}
