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
	case []any:
		return "list"
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
	case Lit:
		return scalarKind(typed.V)
	case Concat:
		return "string"
	case Arith:
		// CEL has no mixed-type arithmetic, so one number operand types the result
		// (cerbos/query-plan-adapters#575).
		if scalarKind(typed.L) == "number" || scalarKind(typed.R) == "number" {
			return "number"
		}
	case Case:
		kinds := caseArmKinds(typed)
		if len(kinds) == 1 {
			for kind := range kinds {
				return kind
			}
		}
	}
	return ""
}

// caseArmKinds is the set of kinds a CASE's arms produce, or nil when any arm's kind is unknown.
// A missing ELSE is NULL, which is UNKNOWN to every comparison rather than a kind of its own.
func caseArmKinds(c Case) map[string]struct{} {
	arms := make([]Expr, 0, len(c.Whens)+1)
	for _, w := range c.Whens {
		arms = append(arms, w.Then)
	}
	if c.Else != nil {
		arms = append(arms, c.Else)
	}
	kinds := make(map[string]struct{}, len(arms))
	for _, arm := range arms {
		kind := scalarKind(arm)
		if kind == "" {
			return nil
		}
		kinds[kind] = struct{}{}
	}
	return kinds
}

// distributeMixedCase lowers a comparison against a CASE whose arms have known, different kinds
// (a ternary whose branches differ in type) arm by arm, so each arm keeps its own answer: an arm
// of the other operand's type compares, and an arm of any other type is the mixed-type answer.
// Compared whole, the CASE would coerce its arms to one type, or fail to execute on PostgreSQL.
func distributeMixedCase(op CmpOp, l, r value) (Expr, bool, error) {
	c, onLeft := l.(Case)
	other := r
	if !onLeft {
		var ok bool
		if c, ok = r.(Case); !ok {
			return nil, false, nil
		}
		other = l
	}
	if len(caseArmKinds(c)) <= 1 {
		return nil, false, nil
	}
	out := Case{Whens: make([]When, 0, len(c.Whens))}
	compare := func(arm Expr) (Expr, error) {
		if onLeft {
			return applyComparison(op, arm, other)
		}
		return applyComparison(op, other, arm)
	}
	for _, w := range c.Whens {
		then, err := compare(w.Then)
		if err != nil {
			return nil, true, err
		}
		out.Whens = append(out.Whens, When{Cond: w.Cond, Then: then})
	}
	if c.Else != nil {
		els, err := compare(c.Else)
		if err != nil {
			return nil, true, err
		}
		out.Else = els
	}
	// An all-NULL CASE resolves to text in PostgreSQL and cannot compose with NOT.
	if caseIsAllNull(out) {
		return Lit{V: nil}, true, nil
	}
	return out, true, nil
}

func caseIsAllNull(c Case) bool {
	for _, w := range c.Whens {
		if lit, ok := w.Then.(Lit); !ok || lit.V != nil {
			return false
		}
	}
	lit, ok := c.Else.(Lit)
	return c.Else == nil || (ok && lit.V == nil)
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
