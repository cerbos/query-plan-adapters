// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package queryplan

import (
	"fmt"
	"strings"
)

// presence lowers `has(x)`, the presence test the planner emits from Cerbos 0.57.0 for an
// attribute it does not fold into a guard: whether the caller's attributes carry the key at all.
//
// Presence is a property of the caller's null convention, not of the column. An attribute sent as
// an explicit null is present on every row; one omitted when its column is NULL is present exactly
// when the column is not NULL. A collection element's column follows the omitted convention unless
// it declares otherwise, as the collection macros already assume. A to-one relation is present
// when its row exists.
//
// has() never errors on a missing value, so the leaf is two-valued and NOT over it is exact. The
// one error it raises is selecting through a missing to-one hop (`has(parent.x)` on a parentless
// row), which denies under both polarities, so a missing hop BEFORE the tested key yields UNKNOWN.
func (b *builder) presence(n *node, m Mapper, negated bool) (Expr, error) {
	if len(n.operands) != unaryOperands || !n.operands[0].isVariable() {
		return nil, fmt.Errorf("'has' requires exactly one attribute reference")
	}
	reference := n.operands[0].variable
	entry, ok := m.Resolve(reference)
	if !ok {
		return nil, fmt.Errorf("attribute does not exist in the attribute column map: %s", reference)
	}
	if entry.Relation != nil {
		return nil, fmt.Errorf(
			"has(%s) tests a collection: a presence test over a to-many relation has no SQL form "+
				"that matches what the caller sends for it", reference,
		)
	}

	omitted := entry.NullConvention == NullConventionOmitted ||
		(entry.NullConvention == NullConventionUnset &&
			(b.opts.NullRepresentation == NullOmitted || isElementReference(m, reference)))

	var out Expr
	switch rel := entry.ScalarRelation; {
	case rel != nil && entry.Column == "":
		// The relation itself: its row exists. Only the hops before it can be missing.
		out = b.requireHops(rel, entry.Qualifier, b.relationScope(rel, b.newAlias(), entry.Qualifier).exists(nil))
	case rel != nil:
		var then Expr = BoolConst{V: true}
		if omitted {
			then = IsNull{X: b.scalarThroughHop(entry), Negate: true}
		}
		rowExists := b.relationScope(rel, b.newAlias(), entry.Qualifier).exists(nil)
		out = Case{Whens: []When{{Cond: rowExists, Then: then}}}
	case omitted:
		v, err := b.resolveVariable(reference, m)
		if err != nil {
			return nil, err
		}
		column, err := asExpr(v)
		if err != nil {
			return nil, err
		}
		out = IsNull{X: column, Negate: true}
	default:
		out = BoolConst{V: true}
	}
	return negate(out, negated), nil
}

// isElementReference reports whether reference reads a field of a collection element bound by an
// enclosing lambda, rather than an attribute of the resource.
func isElementReference(m Mapper, reference string) bool {
	for {
		s, ok := m.(scopedMapper)
		if !ok {
			return false
		}
		if strings.HasPrefix(reference, s.variable+".") {
			return true
		}
		m = s.parent
	}
}
