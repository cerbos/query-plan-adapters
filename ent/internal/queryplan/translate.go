// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package queryplan

import (
	"fmt"
	"strconv"

	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"
)

// NullRepresentation declares how the caller represents a NULL column when building the
// attributes it sends to check().
//
// The planner emits the same `eq(attr, null)` node either way, so the plan cannot reveal which
// convention is in use and the adapter has to be told. See
// https://github.com/cerbos/query-plan-adapters/issues/302.
type NullRepresentation uint8

const (
	// NullExplicit means a NULL column is sent as an explicit null attribute. CEL compares
	// `null == null`, so IS NULL selects exactly the rows check() allows. This is the default
	// and the historical translation.
	NullExplicit NullRepresentation = iota
	// NullOmitted means a NULL column sends no attribute at all. CEL then raises a
	// missing-attribute error, which Cerbos treats as a deny, so a filter that *selects* NULL
	// rows returns rows the PDP denies. Null comparison operands are rejected rather than
	// translated.
	NullOmitted
)

// Options configures a translation.
type Options struct {
	// RootTable qualifies columns on the resource table being filtered.
	RootTable string
	// NullRepresentation declares the caller's NULL convention.
	NullRepresentation NullRepresentation
}

// Build lowers a plan condition into the abstract expression tree.
func Build(cond *enginev1.PlanResourcesFilter_Expression_Operand, m Mapper, opts Options) (Expr, error) {
	root, err := decodeOperand(cond)
	if err != nil {
		return nil, err
	}

	if opts.NullRepresentation == NullOmitted {
		if err := assertNoNullOperands(root); err != nil {
			return nil, err
		}
	}

	b := &builder{opts: opts}
	return b.predicate(root, rootMapper{parent: m, table: opts.RootTable}, scope{qualifier: opts.RootTable}, false)
}

// scope is the row the current expression reads columns from: the resource table at the top
// level, or a collection element's alias inside a lambda body.
type scope struct {
	qualifier string
}

type builder struct {
	opts   Options
	aliasN int
}

func (b *builder) newAlias() string {
	b.aliasN++
	return "cerbos_rel_" + strconv.Itoa(b.aliasN)
}

// Operand counts the planner emits for the shapes this translator understands. Naming them keeps
// the arity checks self-describing at the call sites, which are otherwise a wall of integers.
const (
	unaryOperands   = 1
	binaryOperands  = 2
	ternaryOperands = 3
)

// Operators whose semantics do not depend on which operand holds the column. eq/ne are symmetric,
// and value-first `in` (`value in R.attr.list`) still means membership against the collection, so
// all three normalise to column-first. Every OTHER operator keeps its wire (source) order when the
// value comes first — a receiver-style string match would otherwise swap haystack and needle.
var orderInsensitive = map[string]bool{"eq": true, "ne": true, "in": true}

var comparisonOps = map[string]CmpOp{
	"eq": OpEq, "ne": OpNe, "lt": OpLt, "le": OpLe, "gt": OpGt, "ge": OpGe,
}

var arithmeticOps = map[string]ArithOp{
	"add": OpAdd, "sub": OpSub, "mult": OpMult, "div": OpDiv, "mod": OpMod,
}

// Operators whose second operand is a lambda binding an iteration variable.
var lambdaBinding = map[string]bool{
	"exists": true, "exists_one": true, "all": true, "filter": true, "map": true, "except": true,
}

// Collection macros that fold into a flat boolean combination of their per-element bodies when the
// collection is a literal value list. exists_one/filter/map have no such flattening and fail
// closed instead.
var foldable = map[string]bool{"exists": true, "all": true}

// predicate lowers a node in boolean position. negated is threaded down rather than wrapped
// around the result so that collection macros can push the polarity through the macro itself:
// !exists(body) == all(!body). Applying a SQL NOT around a macro that had already decayed an
// evaluation error to FALSE would turn a deny into an allow.
func (b *builder) predicate(n *node, m Mapper, sc scope, negated bool) (Expr, error) {
	switch {
	case n.isVariable():
		// A bare boolean attribute used as a conjunct, e.g. `R.attr.aBool`.
		v, err := b.resolveVariable(n.variable, m)
		if err != nil {
			return nil, err
		}
		e, err := asExpr(v)
		if err != nil {
			return nil, err
		}
		return negate(e, negated), nil

	case n.isValue():
		truthy, ok := n.value.(bool)
		if !ok {
			return nil, fmt.Errorf("non-boolean constant %v in a boolean position", n.value)
		}
		return BoolConst{V: truthy != negated}, nil
	}

	switch n.operator {
	case "and", "or":
		if len(n.operands) == 0 {
			return nil, fmt.Errorf("'%s' requires at least one operand", n.operator)
		}
		parts := make([]Expr, 0, len(n.operands))
		for _, child := range n.operands {
			p, err := b.predicate(child, m, sc, negated)
			if err != nil {
				return nil, err
			}
			parts = append(parts, p)
		}
		// De Morgan under negation: !(a AND b) == !a OR !b, and vice versa.
		if (n.operator == "and") != negated {
			return and(parts...), nil
		}
		return or(parts...), nil

	case "not":
		if len(n.operands) != unaryOperands {
			return nil, fmt.Errorf("'not' requires exactly one operand")
		}
		return b.predicate(n.operands[0], m, sc, !negated)

	case "if":
		return b.ternaryPredicate(n, m, sc, negated)
	}

	if lambdaBinding[n.operator] {
		return b.collectionMacro(n, m, sc, negated)
	}

	if _, ok := comparisonOps[n.operator]; ok {
		return b.binaryPredicate(n, m, sc, negated)
	}

	switch n.operator {
	case "in", "contains", "startsWith", "endsWith", "ancestorOf", "descendentOf", "overlaps":
		return b.binaryPredicate(n, m, sc, negated)

	case "hasIntersection":
		return b.hasIntersection(n, m, sc, negated)

	case "matches":
		return nil, fmt.Errorf(
			"unsupported operator: matches. CEL's regex dialect (RE2) is not the dialect any SQL " +
				"engine implements, so a translated pattern would accept or reject different strings " +
				"than the PDP",
		)

	case "lambda":
		return nil, fmt.Errorf("lambda expression outside a collection macro")
	}

	return nil, fmt.Errorf("unsupported operator: %s", n.operator)
}

// ternaryPredicate lowers `if(cond, then, else)` used directly as a predicate.
//
// Each branch is guarded by the (un)satisfied condition rather than using ELSE, so an UNKNOWN
// condition leaves both arms unselected and the CASE yields NULL. That keeps the row excluded
// under both polarities, matching CEL treating the missing attribute as an error (a deny).
func (b *builder) ternaryPredicate(n *node, m Mapper, sc scope, negated bool) (Expr, error) {
	if len(n.operands) != ternaryOperands {
		return nil, fmt.Errorf("'if' requires exactly three operands")
	}
	cond, err := b.predicate(n.operands[0], m, sc, false)
	if err != nil {
		return nil, err
	}
	thenExpr, err := b.predicate(n.operands[1], m, sc, negated)
	if err != nil {
		return nil, err
	}
	elseExpr, err := b.predicate(n.operands[2], m, sc, negated)
	if err != nil {
		return nil, err
	}
	return Case{Whens: []When{
		{Cond: cond, Then: thenExpr},
		{Cond: Not{X: cond}, Then: elseExpr},
	}}, nil
}

// binaryPredicate lowers the comparison, membership, string-match and hierarchy operators.
func (b *builder) binaryPredicate(n *node, m Mapper, sc scope, negated bool) (Expr, error) {
	if len(n.operands) != binaryOperands {
		return nil, fmt.Errorf(
			"expected a binary operation: op = %q, # of operands = %d", n.operator, len(n.operands),
		)
	}

	left, right := n.operands[0], n.operands[1]
	operator := n.operator

	// `x in <collection>` where the collection is stored in another table needs a correlated
	// subquery, not an IN list. This is checked before the value-first normalisation below,
	// which would otherwise move the collection out of the operand position CEL puts it in and
	// leave the relation unrecognised (`null in R.attr.tagNames`).
	if operator == "in" {
		if rel, parent, ok := relationFor(m, right); ok {
			return b.membershipOverRelation(left, rel, parent, m, sc, negated)
		}
		if rel, parent, ok := relationFor(m, left); ok {
			return b.membershipOverRelation(right, rel, parent, m, sc, negated)
		}
	}

	// The planner preserves policy source order, so `1 < R.attr.x` arrives value-first and must
	// translate as `x > 1`, not `x < 1` (cerbos/query-plan-adapters#257).
	if left.isValue() && right.isVariable() {
		if cmp, ok := comparisonOps[operator]; ok && cmp.Mirror() != cmp {
			left, right = right, left
			operator = mirroredName(operator)
		} else if orderInsensitive[operator] {
			left, right = right, left
		}
	}

	lv, err := b.value(left, m, sc)
	if err != nil {
		return nil, err
	}
	rv, err := b.value(right, m, sc)
	if err != nil {
		return nil, err
	}

	var out Expr
	switch operator {
	case "in":
		out, err = membership(lv, rv)
	case "contains":
		out, err = stringMatch(lv, rv, true, true)
	case "startsWith":
		out, err = stringMatch(lv, rv, false, true)
	case "endsWith":
		out, err = stringMatch(lv, rv, true, false)
	case "ancestorOf":
		out, err = ancestorOf(lv, rv)
	case "descendentOf":
		out, err = descendentOf(lv, rv)
	case "overlaps":
		out, err = hierarchyOverlaps(lv, rv)
	default:
		out, err = compare(comparisonOps[operator], lv, rv)
	}
	if err != nil {
		return nil, err
	}

	return negate(out, negated), nil
}

func mirroredName(op string) string {
	switch op {
	case "lt":
		return "gt"
	case "gt":
		return "lt"
	case "le":
		return "ge"
	case "ge":
		return "le"
	default:
		return op
	}
}

// membershipOverRelation lowers `<value> in R.attr.<collection>` where the collection is stored in
// a related table.
func (b *builder) membershipOverRelation(needle *node, rel *Relation, parent string, m Mapper, sc scope, negated bool) (Expr, error) {
	if rel.Field == nil {
		return nil, fmt.Errorf(
			"membership against a relation requires the relation to map its element column " +
				"(set Field on the relation)",
		)
	}

	needleValue, err := b.value(needle, m, sc)
	if err != nil {
		return nil, err
	}

	alias := b.newAlias()
	elementCol := Column{Qualifier: alias, Name: rel.Field.Column}

	body, err := elementMatches(elementCol, needleValue)
	if err != nil {
		return nil, err
	}

	// Membership is an `exists` fold, so it carries the same three-valued semantics: a NULL
	// element makes the comparison UNKNOWN, which must stay UNKNOWN rather than decaying to
	// false — otherwise `!(x in tagNames)` would allow a row the PDP denies.
	return b.triStateExists(rel, alias, parent, body, existsSemantics, negated), nil
}

// elementMatches builds the per-element predicate for membership against a stored collection.
//
// The corpus sends collection projections such as `tagNames` under the EXPLICIT-null convention,
// so a NULL element is a real null member rather than a missing one and `null in tagNames` has to
// be true. SQL equality never matches two NULLs, so the both-null case is spelled out — and only
// when the needle is itself a column, since a literal null needle collapses to a plain IS NULL.
func elementMatches(element Column, needle value) (Expr, error) {
	if needle == nil {
		return IsNull{X: element}, nil
	}

	needleExpr, isColumn := needle.(Column)
	if !isColumn {
		return compare(OpEq, element, needle)
	}

	// Null-safe: a null element and a null needle are equal, and a null on one side alone is a
	// mismatch rather than UNKNOWN. Plain `=` would leave those rows UNKNOWN, which survives the
	// enclosing negation and would drop them from `!(x in coll)` even though CEL allows them.
	return NotDistinct{L: element, R: needleExpr}, nil
}

// macroSemantics captures the truth table of a collection macro.
type macroSemantics struct {
	// witness is the per-element state that decides the macro outright.
	witness TruthValue
	// witnessResult is the macro's value when such an element exists.
	witnessResult bool
	// defaultResult is the macro's value when no witness and no error element exists.
	defaultResult bool
}

var (
	// exists is `||` folded: a true element absorbs errors, otherwise an erroring element makes
	// the whole macro an error.
	existsSemantics = macroSemantics{witness: TruthTrue, witnessResult: true, defaultResult: false}
	// all is `&&` folded: a false element absorbs errors, otherwise an erroring element makes the
	// whole macro an error.
	allSemantics = macroSemantics{witness: TruthFalse, witnessResult: false, defaultResult: true}
)

// relationScope resolves a relation into the FROM list and correlation predicate of a subquery
// over it. Intermediate hops are joined inside the subquery; only the parent row correlates out.
func (b *builder) relationScope(rel *Relation, alias, parent string) ([]FromItem, Expr) {
	from := make([]FromItem, 0, len(rel.Via)+1)
	from = append(from, FromItem{Table: rel.Table, Alias: alias})
	preds := make([]Expr, 0, len(rel.Via)+1)

	inner := alias
	for _, hop := range rel.Via {
		hopAlias := b.newAlias()
		from = append(from, FromItem{Table: hop.Table, Alias: hopAlias})
		preds = append(preds, Cmp{
			Op: OpEq,
			L:  Column{Qualifier: inner, Name: hop.ChildColumn},
			R:  Column{Qualifier: hopAlias, Name: hop.JoinColumn},
		})
		inner = hopAlias
	}

	preds = append(preds, Cmp{
		Op: OpEq,
		L:  Column{Qualifier: inner, Name: rel.TargetColumn},
		R:  Column{Qualifier: parent, Name: rel.SourceColumn},
	})

	return from, and(preds...)
}

// triStateExists builds the CASE that preserves CEL's three states across a correlated subquery.
func (b *builder) triStateExists(rel *Relation, alias, parent string, body Expr, sem macroSemantics, negated bool) Expr {
	from, correlate := b.relationScope(rel, alias, parent)

	witness := Subquery{
		Kind: SubqueryExists, From: from, Correlate: correlate,
		Where: TruthTest{X: body, Want: sem.witness},
	}
	unknown := Subquery{
		Kind: SubqueryExists, From: from, Correlate: correlate,
		Where: TruthTest{X: body, Want: TruthUnknown},
	}

	triState := Case{
		Whens: []When{
			{Cond: witness, Then: BoolConst{V: sem.witnessResult}},
			{Cond: unknown, Then: Lit{V: nil}},
		},
		Else: BoolConst{V: sem.defaultResult},
	}

	return negate(triState, negated)
}

// collectionMacro lowers exists/all/exists_one/except (and rejects filter/map).
func (b *builder) collectionMacro(n *node, m Mapper, sc scope, negated bool) (Expr, error) {
	if len(n.operands) != binaryOperands {
		return nil, fmt.Errorf("'%s' requires exactly two operands", n.operator)
	}
	collection, lambda := n.operands[0], n.operands[1]

	// A literal value list arrives when the planner could not unroll a macro over a known
	// collection (more than 10 elements; cerbos/cerbos#2570, #2817). Fold it here so the
	// translation does not depend on which side of that threshold the collection landed.
	if collection.isValue() {
		return b.foldValueListMacro(n.operator, collection.value, lambda, m, sc, negated)
	}

	if !collection.isVariable() {
		return nil, fmt.Errorf("'%s' requires a collection attribute or a literal list", n.operator)
	}

	entry, err := requireRelation(m, collection.variable)
	if err != nil {
		return nil, err
	}
	rel := entry.Relation

	body, variable, err := lambdaParts(lambda, n.operator)
	if err != nil {
		return nil, err
	}

	alias := b.newAlias()
	inner := scopedMapper{variable: variable, relation: rel, alias: alias, parent: m}
	innerScope := scope{qualifier: alias}

	bodyExpr, err := b.predicate(body, inner, innerScope, false)
	if err != nil {
		return nil, err
	}

	from, correlate := b.relationScope(rel, alias, entry.Qualifier)

	switch n.operator {
	case "exists":
		return b.triStateExists(rel, alias, entry.Qualifier, bodyExpr, existsSemantics, negated), nil

	case "all":
		return b.triStateExists(rel, alias, entry.Qualifier, bodyExpr, allSemantics, negated), nil

	case "except":
		sub := Subquery{
			Kind: SubqueryExists, From: from, Correlate: correlate,
			Where: Not{X: bodyExpr},
		}
		return negate(sub, negated), nil

	case "exists_one":
		// exists_one never absorbs an erroring element, so the UNKNOWN witness is checked first
		// and only then is the exact-one count decided.
		unknown := Subquery{
			Kind: SubqueryExists, From: from, Correlate: correlate,
			Where: TruthTest{X: bodyExpr, Want: TruthUnknown},
		}
		count := Subquery{
			Kind: SubqueryCount, From: from, Correlate: correlate,
			Where: TruthTest{X: bodyExpr, Want: TruthTrue},
		}
		triState := Case{
			Whens: []When{
				{Cond: unknown, Then: Lit{V: nil}},
				{Cond: Cmp{Op: OpEq, L: count, R: Lit{V: float64(1)}}, Then: BoolConst{V: true}},
			},
			Else: BoolConst{V: false},
		}
		return negate(triState, negated), nil

	case "filter", "map":
		return nil, fmt.Errorf(
			"'%s' produces a collection rather than a boolean; it only translates inside size() "+
				"or hasIntersection(), which give the collection a scalar meaning",
			n.operator,
		)
	}

	return nil, fmt.Errorf("unsupported collection operator: %s", n.operator)
}

// foldValueListMacro folds a macro whose collection operand is a literal value list.
//
// Each substituted body goes back through the ordinary traversal, so comparison semantics —
// three-valued NULL handling, value-first mirroring — are identical to a chain the planner
// unrolled itself.
func (b *builder) foldValueListMacro(operator string, elements any, lambda *node, m Mapper, sc scope, negated bool) (Expr, error) {
	if !foldable[operator] {
		return nil, fmt.Errorf(
			"%s over a literal collection value is not supported; only exists() and all() can be "+
				"folded into a flat filter", operator,
		)
	}

	list, ok := elements.([]any)
	if !ok {
		return nil, fmt.Errorf("%s over a literal collection requires a list value", operator)
	}

	body, variable, err := lambdaParts(lambda, operator)
	if err != nil {
		return nil, err
	}

	// CEL identity over an empty collection: exists() matches nothing, all() matches everything.
	combinesWithOr := (operator == "exists") != negated
	if len(list) == 0 {
		if operator == "exists" {
			return BoolConst{V: negated}, nil
		}
		return BoolConst{V: !negated}, nil
	}

	parts := make([]Expr, 0, len(list))
	for _, element := range list {
		substituted, err := substituteLambdaVariable(body, variable, element)
		if err != nil {
			return nil, err
		}
		p, err := b.predicate(substituted, m, sc, negated)
		if err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}

	if combinesWithOr {
		return or(parts...), nil
	}
	return and(parts...), nil
}

// hasIntersection lowers `a.hasIntersection(b)`.
func (b *builder) hasIntersection(n *node, m Mapper, sc scope, negated bool) (Expr, error) {
	if len(n.operands) != binaryOperands {
		return nil, fmt.Errorf("'hasIntersection' requires exactly two operands")
	}

	left, right := n.operands[0], n.operands[1]

	// One side must be a literal list for this to become a flat filter; the other may be a
	// relation-backed collection.
	listNode, otherNode := left, right
	if !left.isValue() {
		listNode, otherNode = right, left
	}
	if !listNode.isValue() {
		return nil, fmt.Errorf(
			"hasIntersection between two stored collections requires a set-valued join that SQL " +
				"cannot express as a row filter; one side must be a literal list",
		)
	}

	list, ok := listNode.value.([]any)
	if !ok {
		return nil, fmt.Errorf("hasIntersection requires a list value")
	}

	if rel, parent, ok := relationFor(m, otherNode); ok {
		// Any element of the relation that is a member of the literal list witnesses the
		// intersection, so this is an `exists` over the relation.
		if rel.Field == nil {
			return nil, fmt.Errorf(
				"hasIntersection against a relation requires the relation to map its element column",
			)
		}
		alias := b.newAlias()
		body, err := membership(Column{Qualifier: alias, Name: rel.Field.Column}, list)
		if err != nil {
			return nil, err
		}
		return b.triStateExists(rel, alias, parent, body, existsSemantics, negated), nil
	}

	otherValue, err := b.value(otherNode, m, sc)
	if err != nil {
		return nil, err
	}

	if deferred, ok := otherValue.(deferredCollection); ok {
		if !deferred.isMap {
			return nil, fmt.Errorf("hasIntersection() over filter() is not supported; project with map() instead")
		}
		member, err := membership(deferred.body, list)
		if err != nil {
			return nil, err
		}
		// map() errors on any erroring element without absorbing it, so the UNKNOWN guard is
		// checked before the intersection witness.
		triState := Case{
			Whens: []When{
				{
					Cond: Subquery{
						Kind: SubqueryExists, From: deferred.from, Correlate: deferred.correlate,
						Where: IsNull{X: deferred.body},
					},
					Then: Lit{V: nil},
				},
				{
					Cond: Subquery{
						Kind: SubqueryExists, From: deferred.from, Correlate: deferred.correlate,
						Where: TruthTest{X: member, Want: TruthTrue},
					},
					Then: BoolConst{V: true},
				},
			},
			Else: BoolConst{V: false},
		}
		return negate(triState, negated), nil
	}

	out, err := membership(otherValue, list)
	if err != nil {
		return nil, err
	}
	return negate(out, negated), nil
}

// deferredCollection is a filter()/map() held until size() or hasIntersection() consumes it.
//
// Neither macro yields a boolean, and SQL has no way to return a collection into an enclosing
// comparison, so lowering them eagerly is impossible. Holding the correlated scope and the
// per-element expression lets the consuming operator build exactly the subquery it needs.
type deferredCollection struct {
	correlate Expr
	body      Expr
	from      []FromItem
	isMap     bool
}

func (b *builder) deferredCollection(n *node, m Mapper) (value, error) {
	if len(n.operands) != binaryOperands {
		return nil, fmt.Errorf("'%s' requires exactly two operands", n.operator)
	}
	collection, lambda := n.operands[0], n.operands[1]

	entry, err := requireRelation(m, collectionReference(collection))
	if err != nil {
		return nil, err
	}
	rel := entry.Relation

	body, variable, err := lambdaParts(lambda, n.operator)
	if err != nil {
		return nil, err
	}

	alias := b.newAlias()
	inner := scopedMapper{variable: variable, relation: rel, alias: alias, parent: m}
	innerScope := scope{qualifier: alias}

	var bodyExpr Expr
	if n.operator == "map" {
		projected, err := b.value(body, inner, innerScope)
		if err != nil {
			return nil, err
		}
		if bodyExpr, err = asExpr(projected); err != nil {
			return nil, err
		}
	} else if bodyExpr, err = b.predicate(body, inner, innerScope, false); err != nil {
		return nil, err
	}

	from, correlate := b.relationScope(rel, alias, entry.Qualifier)
	return deferredCollection{
		from: from, correlate: correlate, body: bodyExpr, isMap: n.operator == "map",
	}, nil
}

// collectionReference names the attribute a macro iterates, for error messages and lookup.
func collectionReference(n *node) string {
	if n.isVariable() {
		return n.variable
	}
	return ""
}

// value lowers a node in value position.
func (b *builder) value(n *node, m Mapper, sc scope) (value, error) {
	switch {
	case n.isValue():
		return n.value, nil
	case n.isVariable():
		return b.resolveVariable(n.variable, m)
	}

	switch n.operator {
	case "and", "or", "not":
		return b.predicate(n, m, sc, false)

	case "if":
		return b.ternaryValue(n, m, sc)

	case "hierarchy":
		if len(n.operands) == 0 || len(n.operands) > binaryOperands {
			return nil, fmt.Errorf("'hierarchy' requires one or two operands")
		}
		target, err := b.value(n.operands[0], m, sc)
		if err != nil {
			return nil, err
		}
		var delimiter value
		if len(n.operands) == binaryOperands {
			if delimiter, err = b.value(n.operands[1], m, sc); err != nil {
				return nil, err
			}
		}
		return newHierarchy(target, delimiter)

	case "size":
		return b.size(n, m, sc)

	case "filter", "map":
		// Neither produces a boolean, so both are held back until the operator that consumes
		// them — size() for filter, hasIntersection() for map — can give them meaning.
		return b.deferredCollection(n, m)

	case "timestamp":
		if len(n.operands) != unaryOperands {
			return nil, fmt.Errorf("'timestamp' requires exactly one operand")
		}
		operand := n.operands[0]
		temporal := false
		if operand.isVariable() {
			if entry, ok := m.Resolve(operand.variable); ok && entry.ValueType == ValueTimestamp {
				temporal = true
			}
		}
		v, err := b.value(operand, m, sc)
		if err != nil {
			return nil, err
		}
		return parseTimestamp(v, temporal)

	case "string", "double", "int":
		if len(n.operands) != unaryOperands {
			return nil, fmt.Errorf("'%s' requires exactly one operand", n.operator)
		}
		v, err := b.value(n.operands[0], m, sc)
		if err != nil {
			return nil, err
		}
		return castValue(n.operator, v)
	}

	if _, ok := arithmeticOps[n.operator]; ok {
		return b.arithmetic(n, m, sc)
	}

	if lambdaBinding[n.operator] {
		// A macro can appear in value position when it is the condition of a ternary.
		return b.collectionMacro(n, m, sc, false)
	}

	// Anything else in value position is a predicate embedded in a value slot.
	return b.predicate(n, m, sc, false)
}

func (b *builder) ternaryValue(n *node, m Mapper, sc scope) (value, error) {
	if len(n.operands) != ternaryOperands {
		return nil, fmt.Errorf("'if' requires exactly three operands")
	}
	cond, err := b.predicate(n.operands[0], m, sc, false)
	if err != nil {
		return nil, err
	}
	thenValue, err := b.value(n.operands[1], m, sc)
	if err != nil {
		return nil, err
	}
	elseValue, err := b.value(n.operands[2], m, sc)
	if err != nil {
		return nil, err
	}

	// A non-finite arm must stay symbolic so the enclosing comparison can fold it; anything else
	// becomes a guarded CASE with no ELSE, so an UNKNOWN condition yields NULL.
	if isSymbolic(thenValue) || isSymbolic(elseValue) {
		return condValue{cond: cond, then: thenValue, els: elseValue}, nil
	}

	thenExpr, err := asExpr(thenValue)
	if err != nil {
		return nil, err
	}
	elseExpr, err := asExpr(elseValue)
	if err != nil {
		return nil, err
	}
	return Case{Whens: []When{
		{Cond: cond, Then: thenExpr},
		{Cond: Not{X: cond}, Then: elseExpr},
	}}, nil
}

func isSymbolic(v value) bool {
	switch v.(type) {
	case ieeeConst, condValue:
		return true
	default:
		return false
	}
}

// size lowers size() over either a relation (a correlated count) or a string column.
func (b *builder) size(n *node, m Mapper, sc scope) (value, error) {
	if len(n.operands) != unaryOperands {
		return nil, fmt.Errorf("'size' requires exactly one operand")
	}
	operand := n.operands[0]

	if operand.isVariable() {
		if entry, ok := m.Resolve(operand.variable); ok && entry.Relation != nil {
			// size() counts elements without evaluating them, so a NULL element column still
			// counts and no error guard is needed.
			from, correlate := b.relationScope(entry.Relation, b.newAlias(), entry.Qualifier)
			return Subquery{Kind: SubqueryCount, From: from, Correlate: correlate}, nil
		}
	}

	v, err := b.value(operand, m, sc)
	if err != nil {
		return nil, err
	}
	if deferred, ok := v.(deferredCollection); ok {
		if deferred.isMap {
			return nil, fmt.Errorf("size() over map() is not supported; project with filter() instead")
		}
		// CEL's filter never absorbs an erroring element: a single UNKNOWN body poisons the
		// whole count, so the error guard comes before the count rather than after it.
		return Case{
			Whens: []When{{
				Cond: Subquery{
					Kind: SubqueryExists, From: deferred.from, Correlate: deferred.correlate,
					Where: TruthTest{X: deferred.body, Want: TruthUnknown},
				},
				Then: Lit{V: nil},
			}},
			Else: Subquery{
				Kind: SubqueryCount, From: deferred.from, Correlate: deferred.correlate,
				Where: TruthTest{X: deferred.body, Want: TruthTrue},
			},
		}, nil
	}
	if s, ok := v.(string); ok {
		// Fold a constant so the count is exact rather than dialect-dependent. CEL's size() over
		// a string counts unicode code points, which is not what every dialect's length() does.
		return float64(len([]rune(s))), nil
	}
	if list, ok := v.([]any); ok {
		return float64(len(list)), nil
	}
	e, err := asExpr(v)
	if err != nil {
		return nil, err
	}
	return Call{Name: FuncCharLength, Args: []Expr{e}}, nil
}

func (b *builder) arithmetic(n *node, m Mapper, sc scope) (value, error) {
	if len(n.operands) != binaryOperands {
		return nil, fmt.Errorf("'%s' requires exactly two operands", n.operator)
	}
	lv, err := b.value(n.operands[0], m, sc)
	if err != nil {
		return nil, err
	}
	rv, err := b.value(n.operands[1], m, sc)
	if err != nil {
		return nil, err
	}

	if n.operator == "div" {
		return floatDiv(lv, rv)
	}

	op := arithmeticOps[n.operator]

	// Fold constant arithmetic in double precision, matching CEL's number model exactly rather
	// than deferring to the dialect's numeric type.
	if lf, ok := asFloat(lv); ok {
		if rf, ok := asFloat(rv); ok {
			switch op {
			case OpAdd:
				return lf + rf, nil
			case OpSub:
				return lf - rf, nil
			case OpMult:
				return lf * rf, nil
			case OpMod:
				if rf == 0 {
					return nil, fmt.Errorf("modulus by zero in query plan")
				}
				return float64(int64(lf) % int64(rf)), nil
			case OpDiv:
				// Unreachable: division is routed to floatDiv above so that the IEEE cases stay
				// symbolic. Named explicitly so a new operator cannot silently fall through.
			}
		}
	}

	lExpr, err := asExpr(lv)
	if err != nil {
		return nil, err
	}
	rExpr, err := asExpr(rv)
	if err != nil {
		return nil, err
	}
	return Arith{Op: op, L: lExpr, R: rExpr}, nil
}

func castValue(operator string, v value) (value, error) {
	e, err := asExpr(v)
	if err != nil {
		return nil, err
	}
	switch operator {
	case "string":
		return Cast{X: e, To: CastText}, nil
	case "double":
		return Cast{X: e, To: CastFloat}, nil
	default:
		return Cast{X: e, To: CastInt}, nil
	}
}

// resolveVariable maps a plan reference onto storage. A relation reached in a value position has
// no scalar meaning on its own, so it fails closed rather than degrading into a join.
func (b *builder) resolveVariable(reference string, m Mapper) (value, error) {
	entry, ok := m.Resolve(reference)
	if !ok {
		return nil, fmt.Errorf("attribute does not exist in the attribute column map: %s", reference)
	}
	if entry.Relation != nil {
		return nil, fmt.Errorf(
			"attribute %q maps to a collection and cannot be used as a scalar value", reference,
		)
	}
	return Column{Qualifier: entry.Qualifier, Name: entry.Column}, nil
}

// lambdaParts destructures the planner's `lambda(body, variable)` operand.
func lambdaParts(operand *node, operator string) (*node, string, error) {
	if !operand.isExpr() || operand.operator != "lambda" {
		return nil, "", fmt.Errorf("second operand of %s must be a lambda expression", operator)
	}
	if len(operand.operands) != binaryOperands {
		return nil, "", fmt.Errorf("%s supports single-variable lambdas only", operator)
	}
	body, variable := operand.operands[0], operand.operands[1]
	if !variable.isVariable() || variable.variable == "" {
		return nil, "", fmt.Errorf("lambda variable must have a name")
	}
	return body, variable.variable, nil
}

// substituteLambdaVariable replaces a lambda iteration variable with a concrete element.
//
// A bare reference to the variable becomes the element; `variable.path.to.field` drills into the
// element and fails closed when the path is missing. A nested macro whose lambda rebinds the same
// name shadows the outer variable, so substitution only descends into its collection operand.
func substituteLambdaVariable(n *node, variable string, element any) (*node, error) {
	switch {
	case n.isValue():
		return n, nil

	case n.isVariable():
		if n.variable == variable {
			return cloneWithValue(element), nil
		}
		prefix := variable + "."
		if len(n.variable) > len(prefix) && n.variable[:len(prefix)] == prefix {
			current := element
			for _, segment := range splitPath(n.variable[len(prefix):]) {
				obj, ok := current.(map[string]any)
				if !ok {
					return nil, fmt.Errorf(
						"cannot resolve %q: collection element has no field %q", n.variable, segment,
					)
				}
				next, ok := obj[segment]
				if !ok {
					return nil, fmt.Errorf(
						"cannot resolve %q: collection element has no field %q", n.variable, segment,
					)
				}
				current = next
			}
			return cloneWithValue(current), nil
		}
		return n, nil
	}

	if rebindsVariable(n, variable) {
		// The nested lambda rebinds our variable, so it shadows the outer binding and only its
		// collection operand may be substituted.
		collection, err := substituteLambdaVariable(n.operands[0], variable, element)
		if err != nil {
			return nil, err
		}
		return &node{kind: nodeExpression, operator: n.operator, operands: []*node{collection, n.operands[1]}}, nil
	}

	out := &node{kind: nodeExpression, operator: n.operator, operands: make([]*node, 0, len(n.operands))}
	for _, child := range n.operands {
		substituted, err := substituteLambdaVariable(child, variable, element)
		if err != nil {
			return nil, err
		}
		out.operands = append(out.operands, substituted)
	}
	return out, nil
}

func splitPath(path string) []string {
	var out []string
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			out = append(out, path[start:i])
			start = i + 1
		}
	}
	return append(out, path[start:])
}

// assertNoNullOperands rejects every null literal operand under the omitted representation.
//
// The scan matches on the OPERAND, never on an allowlist of operators: a null constant reaches a
// NULL-selecting predicate through more shapes than the obvious eq/ne/in — hasIntersection carries
// one in its value list too — and any operator added later would silently escape a hand-maintained
// list.
//
// The rejection is deliberately wider than the over-granting shapes. `ne(x, null)` on its own is
// aligned, but a leaf cannot tell whether an enclosing `not` will flip IS NOT NULL back into a
// NULL-selecting predicate, so rejecting every null operand is what stays correct under any
// nesting. Narrowing it would require negation-parity tracking.
func assertNoNullOperands(n *node) error {
	if n.isValue() {
		if carriesNull(n.value) {
			return fmt.Errorf(
				"cannot translate a null operand under NullOmitted: a NULL column sends no " +
					"attribute, so Cerbos evaluates the comparison as a missing-attribute error " +
					"(deny) while a NULL-selecting filter would return those rows. Send NULL " +
					"columns as explicit nulls and use NullExplicit, or keep this shape out of the policy",
			)
		}
		return nil
	}
	for _, child := range n.operands {
		if err := assertNoNullOperands(child); err != nil {
			return err
		}
	}
	return nil
}

func carriesNull(v any) bool {
	if v == nil {
		return true
	}
	list, ok := v.([]any)
	if !ok {
		return false
	}
	for _, item := range list {
		if item == nil {
			return true
		}
	}
	return false
}

func negate(e Expr, negated bool) Expr {
	if !negated {
		return e
	}
	if c, ok := e.(BoolConst); ok {
		return BoolConst{V: !c.V}
	}
	return Not{X: e}
}

// relationFor reports the relation a node's variable maps to, if it maps to one at all.
func relationFor(m Mapper, n *node) (rel *Relation, parent string, ok bool) {
	if !n.isVariable() {
		return nil, "", false
	}
	entry, found := m.Resolve(n.variable)
	if !found || entry.Relation == nil {
		return nil, "", false
	}
	return entry.Relation, entry.Qualifier, true
}

// rebindsVariable reports whether n is a collection macro whose lambda binds the same iteration
// variable name, shadowing an enclosing binding of it.
func rebindsVariable(n *node, variable string) bool {
	if !lambdaBinding[n.operator] || len(n.operands) != binaryOperands {
		return false
	}
	nested := n.operands[1]
	if !nested.isExpr() || nested.operator != "lambda" || len(nested.operands) != binaryOperands {
		return false
	}
	v := nested.operands[1]
	return v.isVariable() && v.variable == variable
}
