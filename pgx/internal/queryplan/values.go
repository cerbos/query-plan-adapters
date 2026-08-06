// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package queryplan

import (
	"cmp"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
)

// A resolved operand is either a plain Go constant folded out of the plan (float64, string, bool,
// nil, []any), an Expr, or one of the two symbolic forms below. Keeping constants unlifted lets
// whole comparisons fold at translation time — `"const".contains("other")` never reaches SQL.
type value = any

// ieeeConst is a non-finite CEL double. It is deliberately NOT lowered into SQL: no portable SQL
// literal denotes NaN or an infinity, and PostgreSQL's NaN ordering is not IEEE's. Comparisons
// fold it away instead (see compareLeaf).
type ieeeConst struct {
	v float64
}

// condValue is a ternary held back from lowering so that a non-finite arm can be folded by the
// enclosing comparison rather than bound as a parameter.
type condValue struct {
	cond Expr
	then value
	els  value
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

	// IEEE-754 keeps the sign of a zero, so `n / -0.0` is the OPPOSITE infinity from `n / 0.0`.
	// A CONSTANT denominator carries its sign on the wire (the planner ships `-0` verbatim and
	// protobuf doubles preserve the sign bit), so it must be applied. A COLUMN denominator does
	// not: SQL cannot tell -0.0 from 0.0 and no portable function reads the sign bit, so the
	// positive-zero reading is assumed — see the README's IEEE section.
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
				then: ieeeConst{v: math.Inf(int(denominatorSign))},
				els:  ieeeConst{v: math.Inf(-int(denominatorSign))},
			},
		},
		els: finite,
	}, nil
}

// arithOverConditional distributes a binary arithmetic operator across a retained ternary, so a
// non-finite arm keeps propagating symbolically instead of being lowered to SQL.
//
// `R.attr.aNumber / R.attr.aNumber + 1.0` composes addition on top of a division that is NaN for
// a zero row. Lowering that arm to SQL turns it into `NULL + 1`, and `NULL != 2.0` is UNKNOWN
// where CEL's `NaN != 2.0` is TRUE — the row the PDP allows would be dropped. Returns (nil, false)
// when neither operand is conditional.
func arithOverConditional(op ArithOp, l, r value) (value, bool, error) {
	combine := func(left, right value) (value, error) {
		if lf, ok := asFloat(left); ok {
			if rf, ok := asFloat(right); ok {
				folded, err := foldArithmetic(op, lf, rf)
				if err != nil {
					return nil, err
				}
				if folded != nil {
					return *folded, nil
				}
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
			if rf, ok := asFloat(right); ok {
				return ieeeConst{v: applyIEEE(op, lc.v, rf)}, nil
			}
			return nil, fmt.Errorf(
				"arithmetic combines a non-finite value with a column, which SQL cannot carry",
			)
		}
		if rc, ok := right.(ieeeConst); ok {
			if lf, ok := asFloat(left); ok {
				return ieeeConst{v: applyIEEE(op, lf, rc.v)}, nil
			}
			return nil, fmt.Errorf(
				"arithmetic combines a non-finite value with a column, which SQL cannot carry",
			)
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
		then, err := combine(cv.then, r)
		if err != nil {
			return nil, true, err
		}
		els, err := combine(cv.els, r)
		if err != nil {
			return nil, true, err
		}
		return condValue{cond: cv.cond, then: then, els: els}, true, nil
	}
	if cv, ok := r.(condValue); ok {
		then, err := combine(l, cv.then)
		if err != nil {
			return nil, true, err
		}
		els, err := combine(l, cv.els)
		if err != nil {
			return nil, true, err
		}
		return condValue{cond: cv.cond, then: then, els: els}, true, nil
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
		// Unreachable: arithOverConditional rejects OpMod before substituting an arm.
		return math.NaN()
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

		// CEL follows IEEE: NaN is unequal to everything and unordered against everything.
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

	return BoolConst{V: compareOrdered(op, lv, rv)}, nil
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
	return Cmp{Op: op, L: lExpr, R: rExpr}, nil
}

// membership implements CEL `in`, including explicit-null list elements.
//
// `x in [null, "a"]` must match the rows where x IS NULL as well as those equal to "a", because
// the corpus's explicit-null convention sends a NULL column as a real null attribute. SQL's
// `IN (NULL, 'a')` never matches a NULL row, so the null members become an explicit IS NULL arm.
func membership(x, values value) (Expr, error) {
	members, ok := values.([]any)
	if !ok {
		members = []any{values}
	}

	xExpr, err := asExpr(x)
	if err != nil {
		return nil, err
	}

	nonNull := make([]Expr, 0, len(members))
	hasNull := false
	for _, m := range members {
		if m == nil {
			hasNull = true
			continue
		}
		nonNull = append(nonNull, Lit{V: m})
	}

	var predicates []Expr
	if len(nonNull) > 0 {
		predicates = append(predicates, InList{X: xExpr, Vs: nonNull})
	}
	if hasNull {
		predicates = append(predicates, IsNull{X: xExpr})
	}
	if len(predicates) == 0 {
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
// have to reject the planner's sub-millisecond now() literal — the `ts-window`/`ts-vf` actions are
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
	default:
		return Lit{V: v}, nil
	}
}

// asFloat reports whether v is a numeric constant. Booleans are excluded: CEL does not treat
// them as numbers, and Go would happily compare them if they slipped through.
func asFloat(v value) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	default:
		return 0, false
	}
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
