// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package queryplan

import (
	"errors"
	"fmt"
	"time"
)

// Durations and the timestamp arithmetic over them. SQL has no portable interval type — SQLite
// stores a timestamp as text, and each engine spells interval arithmetic differently — so a
// duration never reaches SQL. It is folded into the constant side of the comparison instead:
// `timestamp(x) + d < t` becomes `x < t - d`, and `timestamp(x).timeSince() > d` becomes
// `x < now - d`.

// clock is the translator's now(), which timeSince() reads. CEL evaluates timeSince() against the
// clock at evaluation time, and translation is when this adapter evaluates.
var clock = time.Now

// durationConst is a CEL duration constant.
type durationConst struct {
	d time.Duration
}

// timestampShift is `x + d`: a temporal expression shifted by a constant duration.
type timestampShift struct {
	x Expr
	d time.Duration
}

// timeSinceValue is `x.timeSince()`, the duration from a temporal expression to now.
type timeSinceValue struct {
	x Expr
}

func (durationConst) isSymbolicValue()  {}
func (timestampShift) isSymbolicValue() {}
func (timeSinceValue) isSymbolicValue() {}

// parseDuration lowers `duration("…")`. CEL parses the string with Go's own duration syntax.
func parseDuration(v value) (value, error) {
	s, ok := v.(string)
	if !ok {
		return nil, errors.New("duration() translates only over a string literal")
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return nil, fmt.Errorf("invalid duration literal %q: %w", s, err)
	}
	return durationConst{d: d}, nil
}

// timeSince lowers `x.timeSince()` over a timestamp() value.
func timeSince(operand *node, v value) (value, error) {
	if t, ok := v.(time.Time); ok {
		return durationConst{d: clock().Sub(t)}, nil
	}
	if operand.isExpr() && operand.operator == "timestamp" {
		if x, ok := v.(Expr); ok {
			return timeSinceValue{x: x}, nil
		}
	}
	return nil, errors.New("timeSince() translates only over a timestamp() value")
}

// temporalArith folds `+` and `-` between timestamps and durations. ok is false when neither
// operand is temporal, leaving the operator to the numeric and string readings.
func temporalArith(op ArithOp, l, r value) (value, bool, error) {
	if !isTemporal(l) && !isTemporal(r) {
		return nil, false, nil
	}
	if op == OpSub {
		d, ok := r.(durationConst)
		if !ok {
			return nil, true, errTemporalArith
		}
		r, op = durationConst{d: -d.d}, OpAdd
	}
	if op != OpAdd {
		return nil, true, errTemporalArith
	}
	if _, lIsDuration := l.(durationConst); lIsDuration {
		if _, rIsDuration := r.(durationConst); !rIsDuration {
			l, r = r, l
		}
	}
	d, ok := r.(durationConst)
	if !ok {
		return nil, true, errTemporalArith
	}

	switch t := l.(type) {
	case durationConst:
		return durationConst{d: t.d + d.d}, true, nil
	case time.Time:
		shifted := t.Add(d.d)
		if shifted.Before(minCELTimestamp) || shifted.After(maxCELTimestamp) {
			return nil, true, errors.New("timestamp arithmetic leaves CEL's supported instant range")
		}
		return shifted, true, nil
	case timestampShift:
		return timestampShift{x: t.x, d: t.d + d.d}, true, nil
	case Column:
		if t.Type == ValueTimestamp {
			return timestampShift{x: t, d: d.d}, true, nil
		}
	case Subquery:
		if col, isColumn := t.Select.(Column); t.Kind == SubqueryScalar && isColumn && col.Type == ValueTimestamp {
			return timestampShift{x: t, d: d.d}, true, nil
		}
	}
	return nil, true, errTemporalArith
}

var errTemporalArith = errors.New(
	"timestamp arithmetic translates only as a timestamp() value plus or minus a constant duration",
)

func isTemporal(v value) bool {
	switch v.(type) {
	case durationConst, timestampShift, timeSinceValue:
		return true
	}
	return false
}

// compareTemporal folds a comparison over the symbolic temporal values. ok is false when neither
// operand is one.
func compareTemporal(op CmpOp, l, r value) (Expr, bool, error) {
	if !isTemporal(l) && !isTemporal(r) {
		return nil, false, nil
	}
	if isTemporal(r) && !isTemporal(l) {
		out, err := compareTemporalLeft(op.Mirror(), r, l)
		return out, true, err
	}
	out, err := compareTemporalLeft(op, l, r)
	return out, true, err
}

func compareTemporalLeft(op CmpOp, l, r value) (Expr, error) {
	switch t := l.(type) {
	case durationConst:
		if rd, ok := r.(durationConst); ok {
			return BoolConst{V: compareOrdered(op, t.d, rd.d)}, nil
		}
		if _, ok := r.(timeSinceValue); ok {
			return compareTemporalLeft(op.Mirror(), r, l)
		}
	case timestampShift:
		// x + d op c  <=>  x op c - d
		if c, ok := r.(time.Time); ok {
			return compare(op, t.x, c.Add(-t.d))
		}
	case timeSinceValue:
		// now - x op d  <=>  x op' now - d, with op mirrored because x enters negated.
		if d, ok := r.(durationConst); ok {
			return compare(op.Mirror(), t.x, clock().UTC().Add(-d.d))
		}
	}
	return nil, errors.New(
		"a duration compares only with a duration, and a shifted timestamp() only with a timestamp constant",
	)
}

// bareTemporalAgainstTimestamp answers a comparison between an attribute mapped ValueTimestamp but
// read bare, without timestamp(), and a timestamp() value.
//
// CEL holds the bare attribute as its RFC 3339 string, and string has no ordering overload against
// a timestamp, so `<` is an error the PDP denies, and `==` is false (different types are unequal).
// Read as an instant, the stored column would answer both.
func (b *builder) bareTemporalAgainstTimestamp(op CmpOp, left, right *node, m Mapper) (Expr, bool, error) {
	bare, other := left, right
	if !bare.isVariable() {
		bare, other = right, left
	}
	if !bare.isVariable() || !yieldsTimestamp(other) {
		return nil, false, nil
	}
	entry, ok := m.Resolve(bare.variable)
	if !ok || entry.Relation != nil || entry.ValueType != ValueTimestamp {
		return nil, false, nil
	}
	if op != OpEq && op != OpNe {
		return Lit{V: nil}, true, nil
	}
	v, err := b.resolveVariable(bare.variable, m)
	if err != nil {
		return nil, true, err
	}
	result := Expr(BoolConst{V: op == OpNe})
	if col, ok := v.(Column); ok && col.ExplicitNull {
		return result, true, nil
	}
	x, err := asExpr(v)
	if err != nil {
		return nil, true, err
	}
	// A NULL attribute is missing, an error rather than a string.
	return Case{Whens: []When{{Cond: IsNull{X: x, Negate: true}, Then: result}}}, true, nil
}

// yieldsTimestamp reports whether n is a timestamp() value, or one shifted by a duration.
func yieldsTimestamp(n *node) bool {
	if !n.isExpr() {
		return false
	}
	switch n.operator {
	case "timestamp":
		return true
	case "add", "sub":
		return len(n.operands) == binaryOperands && yieldsTimestamp(n.operands[0])
	}
	return false
}
