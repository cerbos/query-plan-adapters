// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package queryplan

// scalarKind uses caller declarations when the wire carries no type. An unknown
// column retains the historical SQL behavior; it must not be guessed numeric.
func scalarKind(v value) string {
	switch typed := v.(type) {
	case string:
		return "string"
	case bool:
		return "bool"
	case float64:
		return "number"
	case Subquery:
		if typed.Kind == SubqueryScalar {
			return scalarKind(typed.Select)
		}
	case Column:
		switch typed.Type {
		case ValueString:
			return "string"
		case ValueBool:
			return "bool"
		case ValueNumber:
			return "number"
		case ValueDefault, ValueTimestamp:
			return ""
		}
	}
	return ""
}

// knownNonString reports whether v is known to be something other than a string — a string
// operator over it is a CEL no-overload error.
func knownNonString(v value) bool {
	kind := scalarKind(v)
	return kind != "" && kind != "string"
}

// compareMixedTypes lowers a comparison whose operands have known, different non-null types,
// reporting false when the types do not both declare themselves or agree.
//
// CEL raises a no-overload error for an ordering across types, and answers equality across types
// definitely: different non-null values are unequal, two explicit null values are equal.
func compareMixedTypes(op CmpOp, l, r value) (Expr, bool) {
	lk, rk := scalarKind(l), scalarKind(r)
	if lk == "" || rk == "" || lk == rk {
		return nil, false
	}
	if op != OpEq && op != OpNe {
		// Every row is UNKNOWN, including missing operands. A guarded all-NULL CASE
		// resolves to text in PostgreSQL and cannot compose with boolean CASE arms.
		return Lit{V: nil}, true
	}

	result := mixedTypeEquality(op, l, r)
	// Any operand that is not an explicit-null column is a missing attribute when NULL, which CEL
	// denies: keep it UNKNOWN rather than answering definitely.
	for _, operand := range []value{l, r} {
		if col, ok := operand.(Column); ok && col.ExplicitNull {
			continue
		}
		if valueExpr, ok := operand.(Expr); ok {
			result = Case{Whens: []When{{Cond: IsNull{X: valueExpr, Negate: true}, Then: result}}}
		}
	}
	return result, true
}

// mixedTypeEquality answers eq/ne between values whose declared non-null types differ.
func mixedTypeEquality(op CmpOp, l, r value) Expr {
	left, leftColumn := l.(Column)
	right, rightColumn := r.(Column)
	if !leftColumn || !rightColumn || !left.ExplicitNull || !right.ExplicitNull {
		return BoolConst{V: op == OpNe}
	}
	// Different non-null types are unequal, but two explicit null values are equal.
	result := and(IsNull{X: left}, IsNull{X: right})
	if op == OpNe {
		return Not{X: result}
	}
	return result
}
