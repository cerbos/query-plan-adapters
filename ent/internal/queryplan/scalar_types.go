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
		switch {
		case typed.IsString:
			return "string"
		case typed.IsBool:
			return "bool"
		case typed.IsNumber:
			return "number"
		}
	}
	return ""
}

// mixedTypeResult compares values whose declared non-null types differ.
func mixedTypeResult(op CmpOp, l, r value) Expr {
	if op != OpEq && op != OpNe {
		return Lit{V: nil}
	}
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
