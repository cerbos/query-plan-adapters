// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package queryplan

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

// The shapes that read a list by position or count its elements one by one: `list[k]` over a
// relation declaring Position, the ordered whole-list equality built on it, and the strict counts
// exists_one(), filter() and except() make over a literal list. SQL has no list operand, so each
// is spelled with the scalar subqueries and CASE sums the rest of the translator already renders.

// positionalElement is `list[k]` read from a relation declaring Position: the element row at that
// position, or none.
//
// Read as a plain value it is a correlated scalar subquery, NULL when the position is out of range
// or a to-one hop on the chain is absent — both CEL errors, which NULL propagates as UNKNOWN
// through every operator. The equality family is the exception: a NULL element on the explicit
// convention is a null VALUE CEL compares definitely, and a scalar subquery cannot tell that NULL
// from the out-of-range one. So comparisons and memberships are built over the element column
// inside EXISTS instead (decided), where the row's presence is a fact of its own.
type positionalElement struct {
	s       scope
	element Column
}

func (positionalElement) isSymbolicValue() {}

// scalar reads the element as a correlated scalar subquery.
func (p positionalElement) scalar() Expr {
	return Subquery{Kind: SubqueryScalar, From: p.s.from, Correlate: p.s.correlate, Select: p.element}
}

// decided is body evaluated over the element row: TRUE or FALSE when the row exists and body is,
// NULL when there is no row at that position or body is UNKNOWN there.
func (p positionalElement) decided(body Expr) Expr {
	return Case{Whens: []When{
		{Cond: p.s.exists(TruthTest{X: body, Want: TruthTrue}), Then: BoolConst{V: true}},
		{Cond: p.s.exists(TruthTest{X: body, Want: TruthFalse}), Then: BoolConst{V: false}},
	}}
}

// positional lowers `list[k]`, or `list[k].field` when field is set, over a relation.
//
// A negative position is out of range and a fractional one has no overload: both are CEL errors,
// so they are NULL rather than a refusal, exactly as an in-range position past the list's end is.
func (b *builder) positional(rel *Relation, parent string, k float64, field string) (value, error) {
	if rel.Position == "" {
		return nil, errors.New(
			"a list position has no lowering over this relation: a related table has no row order, " +
				"so the relation must declare Position, the column holding each element's zero-based position",
		)
	}
	var entry *Entry
	if field == "" {
		entry = rel.Field
		if entry == nil {
			return nil, errors.New(
				"an object list element read by position has no scalar value of its own; select one of its fields",
			)
		}
	} else {
		e, ok := rel.Fields[field]
		if !ok || e.Column == "" {
			return nil, fmt.Errorf("list element field %q is not mapped to a column of the element table", field)
		}
		entry = &e
	}
	if k < 0 || k != math.Trunc(k) {
		return Lit{V: nil}, nil
	}
	alias := b.newAlias()
	s := b.relationScope(rel, alias, parent)
	s.correlate = and(s.correlate, Cmp{Op: OpEq, L: Column{Qualifier: alias, Name: rel.Position}, R: Lit{V: k}})
	return positionalElement{s: s, element: elementColumn(alias, entry)}, nil
}

// positionalRead destructures `index(list, k)` and `get-field(index(list, k), field)` over a
// relation, reporting false for every other shape.
func positionalRead(n *node, m Mapper) (rel *Relation, parent string, k float64, field string, ok bool) {
	if !n.isExpr() || len(n.operands) != binaryOperands {
		return nil, "", 0, "", false
	}
	if n.operator == "get-field" {
		read, name := n.operands[0], n.operands[1]
		if !name.isVariable() || name.variable == "" || !read.isExpr() || read.operator != "index" {
			return nil, "", 0, "", false
		}
		rel, parent, k, _, ok = positionalRead(read, m)
		return rel, parent, k, name.variable, ok
	}
	if n.operator != "index" {
		return nil, "", 0, "", false
	}
	container, key := n.operands[0], n.operands[1]
	if !key.isValue() {
		return nil, "", 0, "", false
	}
	k, isNumber := key.value.(float64)
	if !isNumber {
		return nil, "", 0, "", false
	}
	rel, parent, ok = relationFor(m, container)
	return rel, parent, k, "", ok
}

// positionalComparison lowers a comparison with a positional element on either side, reporting
// false when neither side is one.
func positionalComparison(op CmpOp, l, r value) (Expr, bool, error) {
	if p, ok := l.(positionalElement); ok {
		body, err := compare(op, p.element, r)
		if err != nil {
			return nil, true, err
		}
		return p.decided(body), true, nil
	}
	if p, ok := r.(positionalElement); ok {
		body, err := compare(op, l, p.element)
		if err != nil {
			return nil, true, err
		}
		return p.decided(body), true, nil
	}
	return nil, false, nil
}

// listElementVariable binds the element of a rewritten whole-list equality. No plan names it, so
// it can shadow nothing.
const listElementVariable = "__cerbos_list_element"

func exprNode(operator string, operands ...*node) *node {
	return &node{kind: nodeExpression, operator: operator, operands: operands}
}

func variableNode(name string) *node {
	return &node{kind: nodeVariable, variable: name}
}

// wholeListEquality rewrites `coll == [v1, …, vn]` (and `!=`, in either operand order) for a
// stored list into operators that translate, reporting false for any other shape. coll is the
// relation itself, a map() of it projecting the element or one of its fields, or an except() of
// it compared with at most one element. CEL list equality is ordered, so:
//
//   - no element is `size(coll) == 0`;
//   - one element has no order to compare: `size(coll) == 1 && coll.exists(e, e == v1)`;
//   - more read each position, which needs the relation's Position:
//     `size(coll) == n && coll[0] == v1 && … && coll[n-1] == vn`;
//   - a difference compared with one element: `size(diff) == 1` and a kept element equal to it.
//
// Every rewrite goes back through the ordinary traversal, so a NULL element, an absent to-one hop
// and the element type all keep the semantics the macros and positional reads give them.
func wholeListEquality(n *node, m Mapper) (*node, bool, error) {
	if n.operator != "eq" && n.operator != "ne" {
		return nil, false, nil
	}
	coll, literal := n.operands[0], n.operands[1]
	if coll.isValue() {
		coll, literal = literal, coll
	}
	if !literal.isValue() {
		return nil, false, nil
	}
	elements, ok := literal.value.([]any)
	if !ok {
		return nil, false, nil
	}

	var equality *node
	ok = false
	switch {
	case coll.isVariable():
		equality, ok = relationEquality(coll, "", elements, m)
	case coll.isExpr() && coll.operator == "map" && len(coll.operands) == binaryOperands && coll.operands[0].isVariable():
		body, variable, err := lambdaParts(coll.operands[1], "map")
		if err != nil {
			return nil, true, err
		}
		if !body.isVariable() {
			return nil, false, nil
		}
		field := ""
		if body.variable != variable {
			rest, selected := strings.CutPrefix(body.variable, variable+".")
			if !selected || rest == "" || strings.Contains(rest, ".") {
				return nil, false, nil
			}
			field = rest
		}
		equality, ok = relationEquality(coll.operands[0], field, elements, m)
	case coll.isExpr() && coll.operator == "except" && len(coll.operands) == binaryOperands && len(elements) == 1:
		equality, ok = differenceEquality(coll, elements[0], m)
	}
	if !ok {
		return nil, false, nil
	}
	if n.operator == "ne" {
		equality = exprNode("not", equality)
	}
	return equality, true, nil
}

// relationEquality is the equality of a stored list, or of its field projection, with elements.
func relationEquality(listVar *node, field string, elements []any, m Mapper) (*node, bool) {
	if _, _, ok := relationFor(m, listVar); !ok {
		return nil, false
	}
	size := exprNode("eq", exprNode("size", listVar), valueNode(float64(len(elements))))
	switch len(elements) {
	case 0:
		return size, true
	case 1:
		element := variableNode(listElementVariable)
		if field != "" {
			element = variableNode(listElementVariable + "." + field)
		}
		body := exprNode("eq", element, valueNode(elements[0]))
		exists := exprNode("exists", listVar, exprNode("lambda", body, variableNode(listElementVariable)))
		return exprNode("and", size, exists), true
	}
	conjuncts := []*node{size}
	for k, element := range elements {
		read := exprNode("index", listVar, valueNode(float64(k)))
		if field != "" {
			read = exprNode("get-field", read, variableNode(field))
		}
		conjuncts = append(conjuncts, exprNode("eq", read, valueNode(element)))
	}
	return exprNode("and", conjuncts...), true
}

// differenceEquality is `except(list, other) == [v]`: the difference holds one element, and a
// kept element — one not in other — equals v.
func differenceEquality(except *node, v any, m Mapper) (*node, bool) {
	listVar, other := except.operands[0], except.operands[1]
	if _, _, ok := relationFor(m, listVar); !ok {
		return nil, false
	}
	size := exprNode("eq", exprNode("size", except), valueNode(float64(1)))
	element := variableNode(listElementVariable)
	kept := exprNode("and",
		exprNode("eq", element, valueNode(v)),
		exprNode("not", exprNode("in", element, other)),
	)
	exists := exprNode("exists", listVar, exprNode("lambda", kept, variableNode(listElementVariable)))
	return exprNode("and", size, exists), true
}

// strictCount is the number of TRUE parts, NULL when any part is UNKNOWN: the count filter()
// makes over a literal list, which never absorbs an erroring element.
func strictCount(parts []Expr) Expr {
	if len(parts) == 0 {
		return Lit{V: float64(0)}
	}
	unknown := make([]Expr, 0, len(parts))
	var total Expr
	for _, p := range parts {
		unknown = append(unknown, TruthTest{X: p, Want: TruthUnknown})
		one := Expr(Case{Whens: []When{{Cond: p, Then: Lit{V: float64(1)}}}, Else: Lit{V: float64(0)}})
		if total == nil {
			total = one
		} else {
			total = Arith{Op: OpAdd, L: total, R: one}
		}
	}
	return Case{Whens: []When{{Cond: or(unknown...), Then: Lit{V: nil}}}, Else: total}
}

// strictlyOne is exists_one() over the per-element bodies of a literal list: UNKNOWN when any body
// is, since the macro never absorbs an erroring element, otherwise whether exactly one is TRUE.
func strictlyOne(parts []Expr) Expr {
	if len(parts) == 0 {
		return BoolConst{V: false}
	}
	unknown := make([]Expr, 0, len(parts))
	for _, p := range parts {
		unknown = append(unknown, TruthTest{X: p, Want: TruthUnknown})
	}
	return Case{
		Whens: []When{
			{Cond: or(unknown...), Then: Lit{V: nil}},
			{Cond: Cmp{Op: OpEq, L: strictCount(parts), R: Lit{V: float64(1)}}, Then: BoolConst{V: true}},
		},
		Else: BoolConst{V: false},
	}
}

// countedList is a collection computed over a literal list whose only SQL reading is its size:
// the result of filter(), whose kept elements depend on the row.
type countedList struct {
	size Expr
}

func (countedList) isSymbolicValue() {}

// foldLiteralProjection lowers filter() and map() over a literal list. map() substitutes each
// element into the body and yields the resulting list, constant or computed, which the operators
// over a list() constructor already consume; filter() yields the strict count of the elements its
// body keeps.
func (b *builder) foldLiteralProjection(n *node, elements []any, lambda *node, m Mapper) (value, error) {
	body, variable, err := lambdaParts(lambda, n.operator)
	if err != nil {
		return nil, err
	}
	substituted := make([]*node, 0, len(elements))
	for _, element := range elements {
		s, err := substituteLambdaVariable(body, variable, element)
		if err != nil {
			return nil, err
		}
		substituted = append(substituted, s)
	}
	if n.operator == "map" {
		return b.listConstructor(exprNode("list", substituted...), m)
	}
	parts := make([]Expr, 0, len(substituted))
	for _, s := range substituted {
		p, err := b.predicate(s, m, false)
		if err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return countedList{size: strictCount(parts)}, nil
}

// differenceSize lowers `size(except(a, b))`.
//
// A stored list a less a literal list b counts the elements whose membership in b is FALSE, and is
// UNKNOWN when any element's membership is: CEL's except() tests every element and errors on the
// first error. A literal list a less a list b of computed elements counts, per element of a, the
// memberships that are FALSE, UNKNOWN unless every element of b evaluates. Two literal lists fold.
func (b *builder) differenceSize(except *node, m Mapper) (value, error) {
	if len(except.operands) != binaryOperands {
		return nil, errors.New("'except' requires exactly two operands")
	}
	left, right := except.operands[0], except.operands[1]

	if rel, parent, ok := relationFor(m, left); ok {
		if !right.isValue() {
			return nil, errors.New("size() over except() translates a stored list only against a literal list")
		}
		members, isList := right.value.([]any)
		if !isList {
			return nil, errors.New("except() requires a list value")
		}
		if rel.Field == nil {
			return nil, errors.New("except() over a relation requires the relation to map its element column")
		}
		alias := b.newAlias()
		member, err := membership(elementColumn(alias, rel.Field), members)
		if err != nil {
			return nil, err
		}
		s := b.relationScope(rel, alias, parent)
		count := Case{
			Whens: []When{{Cond: s.exists(TruthTest{X: member, Want: TruthUnknown}), Then: Lit{V: nil}}},
			Else:  s.count(TruthTest{X: member, Want: TruthFalse}),
		}
		return b.requireHops(rel, parent, count), nil
	}

	lv, err := b.value(left, m)
	if err != nil {
		return nil, err
	}
	elements, isList := lv.([]any)
	if !isList {
		return nil, errors.New("size() over except() translates a stored list or a literal list only")
	}
	rv, err := b.value(right, m)
	if err != nil {
		return nil, err
	}
	switch other := rv.(type) {
	case []any:
		kept := 0
		for _, element := range elements {
			found := false
			for _, candidate := range other {
				equal, err := constantsEqual(element, candidate)
				if err != nil {
					return nil, err
				}
				if equal {
					found = true
					break
				}
			}
			if !found {
				kept++
			}
		}
		return float64(kept), nil
	case listValue:
		excluded := make([]Expr, 0, len(elements))
		for _, element := range elements {
			member, err := membershipInList(element, other)
			if err != nil {
				return nil, err
			}
			excluded = append(excluded, member)
		}
		// Every element is kept exactly when its membership is FALSE; whenDefined on each
		// membership already makes an erroring other UNKNOWN.
		notMembers := make([]Expr, 0, len(excluded))
		for _, member := range excluded {
			notMembers = append(notMembers, Not{X: member})
		}
		return strictCount(notMembers), nil
	}
	return nil, errors.New("size() over except() translates only against a literal list or a list() of computed elements")
}

// intersectionSize lowers `size(intersect(a, b))` for a stored list on one side and a literal
// list on the other.
//
// Cerbos's intersect() walks the shorter list — the first when they are the same length — and
// keeps each element the longer one contains, duplicates included, so the count turns on which
// list is shorter: walking the literal counts its elements the stored list has, each once per
// occurrence in the literal; walking the stored list counts its rows whose element is in the
// literal. A CASE on the stored list's size picks the walk. Two literal lists fold.
func (b *builder) intersectionSize(intersect *node, m Mapper) (value, error) {
	if len(intersect.operands) != binaryOperands {
		return nil, errors.New("'intersect' requires exactly two operands")
	}
	left, right := intersect.operands[0], intersect.operands[1]
	if left.isValue() && right.isValue() {
		lv, lok := left.value.([]any)
		rv, rok := right.value.([]any)
		if !lok || !rok {
			return nil, errors.New("intersect() requires list values")
		}
		shorter, longer := lv, rv
		if len(lv) > len(rv) {
			shorter, longer = rv, lv
		}
		kept := 0
		for _, element := range shorter {
			for _, candidate := range longer {
				equal, err := constantsEqual(element, candidate)
				if err != nil {
					return nil, err
				}
				if equal {
					kept++
					break
				}
			}
		}
		return float64(kept), nil
	}

	stored, literal := left, right
	rel, parent, ok := relationFor(m, stored)
	if !ok {
		stored, literal = right, left
		if rel, parent, ok = relationFor(m, stored); !ok {
			return nil, errors.New("size() over intersect() translates a stored list against a literal list only")
		}
	}
	if !literal.isValue() {
		return nil, errors.New("size() over intersect() translates a stored list against a literal list only")
	}
	members, isList := literal.value.([]any)
	if !isList {
		return nil, errors.New("intersect() requires a list value")
	}
	if rel.Field == nil {
		return nil, errors.New("intersect() over a relation requires the relation to map its element column")
	}

	// Walking the literal: each member the stored list contains counts once.
	contained := make([]Expr, 0, len(members))
	for _, member := range members {
		alias := b.newAlias()
		match, err := b.elementMatches(elementColumn(alias, rel.Field), member, true)
		if err != nil {
			return nil, err
		}
		contained = append(contained, b.triStateExists(rel, alias, parent, match, existsSemantics, false))
	}
	literalWalk := strictCount(contained)

	// Walking the stored list: each row whose element is a member counts, UNKNOWN when any
	// row's membership is.
	alias := b.newAlias()
	member, err := membership(elementColumn(alias, rel.Field), members)
	if err != nil {
		return nil, err
	}
	s := b.relationScope(rel, alias, parent)
	storedWalk := Case{
		Whens: []When{{Cond: s.exists(TruthTest{X: member, Want: TruthUnknown}), Then: Lit{V: nil}}},
		Else:  s.count(TruthTest{X: member, Want: TruthTrue}),
	}

	// The stored list is walked when it is the shorter, or when it is the first operand and the
	// two are the same length.
	size := b.relationScope(rel, b.newAlias(), parent).count(nil)
	walkStored := Expr(Cmp{Op: OpLt, L: size, R: Lit{V: float64(len(members))}})
	if stored == left {
		walkStored = Cmp{Op: OpLe, L: size, R: Lit{V: float64(len(members))}}
	}
	count := Case{Whens: []When{{Cond: walkStored, Then: storedWalk}}, Else: literalWalk}
	return b.requireHops(rel, parent, count), nil
}
