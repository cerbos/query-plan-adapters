// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package queryplan

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// The list-valued shapes: list literals built by ternaries and `+`, the set functions that reduce to
// a macro over a stored list, and membership in a filter()/map() projection. SQL has no list
// operand, so each of these is rewritten into scalar shapes the rest of the translator already
// lowers, or refused.

// liftListTernary rewrites `op(…, if(c, a, b), …)` as `if(c, op(…, a, …), op(…, b, …))` when the
// ternary yields a list, looking through `+` so a ternary inside a concatenation is lifted too.
//
// SQL has no CASE that yields a list, so the ternary has to sit above the comparison rather than
// inside it. The rewrite is exact in CEL, errors included: an erroring condition errors either
// way, and ternaryPredicate renders it UNKNOWN under both polarities.
func liftListTernary(n *node) (*node, bool) {
	if !n.isExpr() {
		return n, false
	}
	for i, operand := range n.operands {
		if operand.isExpr() && operand.operator == "add" {
			if lifted, ok := liftListTernary(operand); ok {
				operand = lifted
			}
		}
		if !operand.isExpr() || operand.operator != "if" || len(operand.operands) != ternaryOperands {
			continue
		}
		if !yieldsList(operand.operands[1]) && !yieldsList(operand.operands[2]) {
			continue
		}
		withArm := func(arm *node) *node {
			operands := slices.Clone(n.operands)
			operands[i] = arm
			return &node{kind: nodeExpression, operator: n.operator, operands: operands}
		}
		return &node{kind: nodeExpression, operator: "if", operands: []*node{
			operand.operands[0], withArm(operand.operands[1]), withArm(operand.operands[2]),
		}}, true
	}
	return n, false
}

// yieldsList reports whether n is a list literal, or a `+` or ternary built from one.
func yieldsList(n *node) bool {
	if n.isValue() {
		_, list := n.value.([]any)
		return list
	}
	if !n.isExpr() {
		return false
	}
	switch n.operator {
	case "add":
		return slices.ContainsFunc(n.operands, yieldsList)
	case "if":
		return len(n.operands) == ternaryOperands && (yieldsList(n.operands[1]) || yieldsList(n.operands[2]))
	}
	return false
}

// concatenatedHaystack flattens the haystack of `x in a + b + …` into its parts when at least one
// part is a stored list, so the membership can become a disjunction of memberships.
//
// `x in a + b` is `x in a || x in b` in CEL whenever both lists evaluate. The one way a stored list
// fails to evaluate is an absent to-one hop on its chain, which errors the whole concatenation while
// the disjunction would let the other part's TRUE absorb it, so a chained relation is not flattened.
func concatenatedHaystack(m Mapper, n *node) ([]*node, bool) {
	parts := make([]*node, 0, binaryOperands)
	stored := false
	var walk func(*node) bool
	walk = func(part *node) bool {
		switch {
		case part.isExpr() && part.operator == "add" && len(part.operands) == binaryOperands:
			return walk(part.operands[0]) && walk(part.operands[1])
		case part.isValue():
			if _, list := part.value.([]any); !list {
				return false
			}
		default:
			rel, _, ok := relationFor(m, part)
			if !ok || len(rel.Via) > 0 {
				return false
			}
			stored = true
		}
		parts = append(parts, part)
		return true
	}
	if !n.isExpr() || n.operator != "add" || !walk(n) || !stored {
		return nil, false
	}
	return parts, true
}

// emptyListComparison rewrites `except(a, b) == []` as `a.isSubset(b)` and `intersect(a, b) == []`
// as `!a.hasIntersection(b)` (and `!=` as their negations), in either operand order. Both are CEL
// identities: a difference is empty exactly when every element is in the other list, and an
// intersection is empty exactly when no element is, whatever the duplicates.
func emptyListComparison(n *node) (*node, bool) {
	if n.operator != "eq" && n.operator != "ne" {
		return nil, false
	}
	setOp, other := n.operands[0], n.operands[1]
	if !setOp.isExpr() {
		setOp, other = other, setOp
	}
	if !other.isValue() || !setOp.isExpr() || len(setOp.operands) != binaryOperands {
		return nil, false
	}
	if list, ok := other.value.([]any); !ok || len(list) != 0 {
		return nil, false
	}

	var empty *node
	switch setOp.operator {
	case "except":
		empty = &node{kind: nodeExpression, operator: "isSubset", operands: setOp.operands}
	case "intersect":
		empty = &node{kind: nodeExpression, operator: "not", operands: []*node{
			{kind: nodeExpression, operator: "hasIntersection", operands: setOp.operands},
		}}
	default:
		return nil, false
	}
	if n.operator == "ne" {
		empty = &node{kind: nodeExpression, operator: "not", operands: []*node{empty}}
	}
	return empty, true
}

// isSubset lowers `a.isSubset(b)` for a stored list a and a literal list b: every element of a is a
// member of b, which is an `all` over the relation.
func (b *builder) isSubset(n *node, m Mapper, negated bool) (Expr, error) {
	if len(n.operands) != binaryOperands {
		return nil, fmt.Errorf("'isSubset' requires exactly two operands")
	}
	sub, super := n.operands[0], n.operands[1]
	rel, parent, ok := relationFor(m, sub)
	if !ok || !super.isValue() {
		return nil, errors.New(
			"isSubset translates only a stored list against a literal list: SQL has no set " +
				"containment between two stored collections or over a computed list",
		)
	}
	list, ok := super.value.([]any)
	if !ok {
		return nil, fmt.Errorf("isSubset requires a list value")
	}
	if rel.Field == nil {
		return nil, fmt.Errorf("isSubset against a relation requires the relation to map its element column")
	}
	alias := b.newAlias()
	body, err := membership(elementColumn(alias, rel.Field), list)
	if err != nil {
		return nil, err
	}
	return b.triStateExists(rel, alias, parent, body, allSemantics, negated), nil
}

// membershipInDeferred lowers `needle in <filter()/map() result>` for a constant needle.
//
// Neither macro absorbs an erroring element, so an element whose filter predicate is UNKNOWN, or
// whose projection is NULL (a missing field), makes the whole membership UNKNOWN; only then does a
// matching element decide it. A column needle is refused: a NULL there may be a missing attribute,
// which must stay an error rather than read as "no element matches".
func (b *builder) membershipInDeferred(needle value, d deferredCollection) (Expr, error) {
	switch needle.(type) {
	case string, float64, bool:
	default:
		return nil, fmt.Errorf(
			"membership in a %s() result translates only for a constant string, number or "+
				"boolean needle", d.macro(),
		)
	}

	var unknown, witness Expr
	if d.isMap {
		match, err := compare(OpEq, d.body, needle)
		if err != nil {
			return nil, err
		}
		unknown = IsNull{X: d.body}
		witness = TruthTest{X: match, Want: TruthTrue}
	} else {
		if d.element == nil {
			return nil, fmt.Errorf(
				"membership in a filter() result requires the relation to map its element column",
			)
		}
		match, err := compare(OpEq, d.element, needle)
		if err != nil {
			return nil, err
		}
		unknown = TruthTest{X: d.body, Want: TruthUnknown}
		witness = and(TruthTest{X: d.body, Want: TruthTrue}, TruthTest{X: match, Want: TruthTrue})
	}

	triState := Case{
		Whens: []When{
			{Cond: d.exists(unknown), Then: Lit{V: nil}},
			{Cond: d.exists(witness), Then: BoolConst{V: true}},
		},
		Else: BoolConst{V: false},
	}
	return b.requireHops(d.rel, d.parent, triState), nil
}

// constantListsEqual folds CEL equality between two list literals. Elements compare as CEL compares
// them: numbers by value, values of different types unequal, nested lists element by element. A map
// element is refused rather than guessed at.
func constantListsEqual(l, r []any) (bool, error) {
	if len(l) != len(r) {
		return false, nil
	}
	for i := range l {
		equal, err := constantsEqual(l[i], r[i])
		if err != nil || !equal {
			return false, err
		}
	}
	return true, nil
}

func constantsEqual(l, r any) (bool, error) {
	switch lt := l.(type) {
	case nil:
		return r == nil, nil
	case string:
		rt, ok := r.(string)
		return ok && lt == rt, nil
	case bool:
		rt, ok := r.(bool)
		return ok && lt == rt, nil
	case float64:
		rt, ok := r.(float64)
		return ok && lt == rt, nil
	case []any:
		rt, ok := r.([]any)
		if !ok {
			return false, nil
		}
		return constantListsEqual(lt, rt)
	}
	return false, fmt.Errorf("list equality over a %T element has no constant folding", l)
}

// structLiteral folds `struct(set-field(k, v), …)` into a map when every key is a string constant
// and every value a constant. Anything else has no constant reading and is refused.
func structLiteral(n *node) (map[string]any, error) {
	out := make(map[string]any, len(n.operands))
	for _, field := range n.operands {
		if !field.isExpr() || field.operator != "set-field" || len(field.operands) != binaryOperands {
			return nil, errors.New("struct() translates only as a map literal of set-field() entries")
		}
		key, value := field.operands[0], field.operands[1]
		k, ok := key.value.(string)
		if !key.isValue() || !ok {
			return nil, errors.New("a map literal translates only with constant string keys")
		}
		if !value.isValue() {
			return nil, errors.New("a map literal translates only with constant values")
		}
		if _, duplicate := out[k]; duplicate {
			return nil, fmt.Errorf("map literal repeats the key %q, which CEL rejects", k)
		}
		out[k] = value.value
	}
	return out, nil
}

// listValue is `list(e1, …)` holding at least one element that is not a constant.
//
// Building the list evaluates every element, so one erroring element errors the whole list, even
// where a macro or a membership over it would otherwise have been decided by another element.
// defined is the conjunction that every element evaluates — a NULL that is not an explicit null
// is a missing attribute or an erroring expression — and nil when every element always does.
type listValue struct {
	defined  Expr
	elements []value
	nodes    []*node
}

func (listValue) isSymbolicValue() {}

// listConstructor lowers `list(e1, …)`. A list of constants folds to a literal; anything else is
// held as a listValue for the membership or macro that consumes it.
func (b *builder) listConstructor(n *node, m Mapper) (value, error) {
	elements := make([]value, 0, len(n.operands))
	constant := true
	for _, operand := range n.operands {
		v, err := b.value(operand, m)
		if err != nil {
			return nil, err
		}
		if !isConstant(v) {
			constant = false
		}
		elements = append(elements, v)
	}
	if constant {
		return elements, nil
	}

	var defined []Expr
	for _, v := range elements {
		if isConstant(v) {
			continue
		}
		x, err := asExpr(v)
		if err != nil {
			return nil, err
		}
		if _, explicit := explicitNullColumn(x); explicit {
			continue
		}
		defined = append(defined, IsNull{X: x, Negate: true})
	}
	lv := listValue{elements: elements, nodes: n.operands}
	if len(defined) > 0 {
		lv.defined = and(defined...)
	}
	return lv, nil
}

func isConstant(v value) bool {
	switch v.(type) {
	case nil, string, float64, bool, []any, map[string]any:
		return true
	}
	return false
}

// whenDefined makes e UNKNOWN unless every element of the list evaluates.
func (l listValue) whenDefined(e Expr) Expr {
	if l.defined == nil {
		return e
	}
	return Case{Whens: []When{{Cond: l.defined, Then: e}}}
}

// membershipInList lowers `needle in list(e1, …)` as the disjunction of its equalities, UNKNOWN
// unless the whole list evaluates.
func membershipInList(needle value, l listValue) (Expr, error) {
	parts := make([]Expr, 0, len(l.elements))
	for _, element := range l.elements {
		eq, err := compare(OpEq, needle, element)
		if err != nil {
			return nil, err
		}
		parts = append(parts, eq)
	}
	return l.whenDefined(or(parts...)), nil
}

// foldListConstructorMacro folds exists()/all() over `list(e1, …)` into the flat combination of
// its bodies, each with the lambda variable bound to one element expression, UNKNOWN unless the
// whole list evaluates.
func (b *builder) foldListConstructorMacro(operator string, l listValue, lambda *node, m Mapper, negated bool) (Expr, error) {
	if !foldable[operator] {
		return nil, fmt.Errorf(
			"%s over a list() of computed elements is not supported; only exists() and all() can "+
				"be folded into a flat filter", operator,
		)
	}
	body, variable, err := lambdaParts(lambda, operator)
	if err != nil {
		return nil, err
	}
	parts := make([]Expr, 0, len(l.nodes))
	for _, element := range l.nodes {
		substituted, err := substituteLambdaNode(body, variable, element)
		if err != nil {
			return nil, err
		}
		p, err := b.predicate(substituted, m, negated)
		if err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	if (operator == "exists") != negated {
		return l.whenDefined(or(parts...)), nil
	}
	return l.whenDefined(and(parts...)), nil
}

// substituteLambdaNode binds a lambda variable to an element expression. A field selected off the
// variable is refused: the element is an expression, not a literal object to drill into.
func substituteLambdaNode(n *node, variable string, element *node) (*node, error) {
	switch {
	case n.isValue():
		return n, nil
	case n.isVariable():
		if n.variable == variable {
			return element, nil
		}
		if strings.HasPrefix(n.variable, variable+".") {
			return nil, fmt.Errorf("cannot select %q from a computed list element", n.variable)
		}
		return n, nil
	}

	if rebindsVariable(n, variable) {
		collection, err := substituteLambdaNode(n.operands[0], variable, element)
		if err != nil {
			return nil, err
		}
		return &node{kind: nodeExpression, operator: n.operator, operands: []*node{collection, n.operands[1]}}, nil
	}

	out := &node{kind: nodeExpression, operator: n.operator, operands: make([]*node, 0, len(n.operands))}
	for _, child := range n.operands {
		substituted, err := substituteLambdaNode(child, variable, element)
		if err != nil {
			return nil, err
		}
		out.operands = append(out.operands, substituted)
	}
	return out, nil
}

// mapLiteralLookup lowers `{k: v, …}[x] == c` (and `!=`) for a map literal with string keys and a
// constant c: x is one of the keys whose value equals c, provided x is a key at all. A missing key
// is a CEL error, so the comparison is UNKNOWN wherever x is not a key.
func (b *builder) mapLiteralLookup(op CmpOp, n *node, m Mapper) (Expr, bool, error) {
	lookup, other := n.operands[0], n.operands[1]
	if !other.isValue() {
		lookup, other = other, lookup
	}
	if !other.isValue() || !lookup.isExpr() || lookup.operator != "index" || len(lookup.operands) != binaryOperands {
		return nil, false, nil
	}
	container := lookup.operands[0]
	if !container.isExpr() || container.operator != "struct" {
		return nil, false, nil
	}
	entries, err := structLiteral(container)
	if err != nil {
		return nil, true, err
	}
	key, err := b.value(lookup.operands[1], m)
	if err != nil {
		return nil, true, err
	}

	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	present := make([]any, 0, len(keys))
	matching := make([]any, 0, len(keys))
	for _, k := range keys {
		present = append(present, k)
		equal, err := constantsEqual(entries[k], other.value)
		if err != nil {
			return nil, true, err
		}
		if equal {
			matching = append(matching, k)
		}
	}

	isKey, err := membership(key, present)
	if err != nil {
		return nil, true, err
	}
	hit, err := membership(key, matching)
	if err != nil {
		return nil, true, err
	}
	return Case{Whens: []When{{Cond: isKey, Then: negate(hit, op == OpNe)}}}, true, nil
}
