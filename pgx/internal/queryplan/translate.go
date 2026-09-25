// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package queryplan

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

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

	if strings.HasPrefix(opts.RootTable, aliasPrefix) {
		return nil, fmt.Errorf(
			"resource table %q collides with the %q prefix reserved for generated subquery aliases",
			opts.RootTable, aliasPrefix,
		)
	}

	m = guardedMapper{parent: m}

	// Always: the call-level option is only the fallback now, and an attribute can declare
	// NullConventionOmitted while the call declares NullExplicit.
	if err := assertNoNullOperands(root, m, opts.NullRepresentation); err != nil {
		return nil, err
	}

	b := &builder{opts: opts}
	return b.predicate(root, rootMapper{parent: m, table: opts.RootTable}, false)
}

type builder struct {
	opts   Options
	aliasN int
}

// aliasPrefix names the aliases generated for correlated subqueries. A caller whose resource table
// is itself named with this prefix is rejected up front: an inner alias would shadow the outer
// table, the correlation would compare the subquery's own row against itself, and the filter would
// match rows the PDP denies — silently, since the SQL stays valid.
const aliasPrefix = "cerbos_rel_"

func (b *builder) newAlias() string {
	b.aliasN++
	return aliasPrefix + strconv.Itoa(b.aliasN)
}

// Operand counts the planner emits for the shapes this translator understands. Naming them keeps
// the arity checks self-describing at the call sites, which are otherwise a wall of integers.
const (
	unaryOperands   = 1
	binaryOperands  = 2
	ternaryOperands = 3
)

var comparisonOps = map[string]CmpOp{
	"eq": OpEq, "ne": OpNe, "lt": OpLt, "le": OpLe, "gt": OpGt, "ge": OpGe,
}

// binaryOperators lowers the non-comparison binary operators once binaryPredicate has resolved
// and normalised their operands. Adding a binary operator is one entry here.
var binaryOperators = map[string]func(l, r value) (Expr, error){
	"in":           membership,
	"contains":     func(l, r value) (Expr, error) { return stringMatch(l, r, true, true) },
	"startsWith":   func(l, r value) (Expr, error) { return stringMatch(l, r, false, true) },
	"endsWith":     func(l, r value) (Expr, error) { return stringMatch(l, r, true, false) },
	"ancestorOf":   ancestorOf,
	"descendentOf": descendentOf,
	"overlaps":     hierarchyOverlaps,
}

var arithmeticOps = map[string]ArithOp{
	"add": OpAdd, "sub": OpSub, "mult": OpMult, "div": OpDiv, "mod": OpMod,
}

// Operators whose second operand is a lambda binding an iteration variable.
var lambdaBinding = map[string]bool{
	"exists": true, "exists_one": true, "all": true, "filter": true, "map": true,
}

// Collection macros that fold into a flat boolean combination of their per-element bodies when the
// collection is a literal value list. exists_one/filter/map have no such flattening and fail
// closed instead.
var foldable = map[string]bool{"exists": true, "all": true}

// predicate lowers a node in boolean position. negated is threaded down rather than wrapped
// around the result so that collection macros can push the polarity through the macro itself:
// !exists(body) == all(!body). Applying a SQL NOT around a macro that had already decayed an
// evaluation error to FALSE would turn a deny into an allow.
func (b *builder) predicate(n *node, m Mapper, negated bool) (Expr, error) {
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
			p, err := b.predicate(child, m, negated)
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
		return b.predicate(n.operands[0], m, !negated)

	case "if":
		return b.ternaryPredicate(n, m, negated)
	}

	if lambdaBinding[n.operator] {
		return b.collectionMacro(n, m, negated)
	}

	if _, ok := comparisonOps[n.operator]; ok {
		return b.binaryPredicate(n, m, negated)
	}
	if _, ok := binaryOperators[n.operator]; ok {
		return b.binaryPredicate(n, m, negated)
	}

	switch n.operator {
	case "hasIntersection":
		return b.hasIntersection(n, m, negated)

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
func (b *builder) ternaryPredicate(n *node, m Mapper, negated bool) (Expr, error) {
	if len(n.operands) != ternaryOperands {
		return nil, fmt.Errorf("'if' requires exactly three operands")
	}
	cond, err := b.predicate(n.operands[0], m, false)
	if err != nil {
		return nil, err
	}
	thenExpr, err := b.predicate(n.operands[1], m, negated)
	if err != nil {
		return nil, err
	}
	elseExpr, err := b.predicate(n.operands[2], m, negated)
	if err != nil {
		return nil, err
	}
	return Case{Whens: []When{
		{Cond: cond, Then: thenExpr},
		{Cond: Not{X: cond}, Then: elseExpr},
	}}, nil
}

// binaryPredicate lowers the comparisons and binaryOperators.
func (b *builder) binaryPredicate(n *node, m Mapper, negated bool) (Expr, error) {
	if len(n.operands) != binaryOperands {
		return nil, fmt.Errorf(
			"expected a binary operation: op = %q, # of operands = %d", n.operator, len(n.operands),
		)
	}

	left, right := n.operands[0], n.operands[1]
	operator := n.operator
	cmpOp, comparison := comparisonOps[operator]
	if comparison && left.isVariable() && right.isVariable() {
		l, lok := m.Resolve(left.variable)
		r, rok := m.Resolve(right.variable)
		if lok && rok && (l.ValueType == ValueTimestamp || r.ValueType == ValueTimestamp) {
			return nil, fmt.Errorf("bare temporal attributes compare RFC 3339 strings in CEL; stored timestamps lose the original spelling; use timestamp() on both operands")
		}
	}

	// `x in <collection>` where the collection is stored in another table needs a correlated
	// subquery, not an IN list. This is checked before the value-first normalisation below,
	// which would otherwise move the collection out of the operand position CEL puts it in and
	// leave the relation unrecognised (`null in R.attr.tagNames`).
	if operator == "in" {
		if rel, parent, ok := relationFor(m, right); ok {
			return b.membershipOverRelation(left, rel, parent, m, negated)
		}
		if rel, parent, ok := relationFor(m, left); ok {
			return b.membershipOverRelation(right, rel, parent, m, negated)
		}
	}

	// The planner preserves policy source order, so `1 < R.attr.x` arrives value-first and must
	// translate as `x > 1`, not `x < 1` (cerbos/query-plan-adapters#257). A comparison normalises
	// to column-first by mirroring (eq/ne mirror to themselves), and value-first `in`
	// (`value in R.attr.list`) still means membership against the collection. Every OTHER operator
	// keeps its wire (source) order — a receiver-style string match would otherwise swap haystack
	// and needle.
	if left.isValue() && right.isVariable() && (comparison || operator == "in") {
		left, right = right, left
		cmpOp = cmpOp.Mirror()
	}

	lv, err := b.value(left, m)
	if err != nil {
		return nil, err
	}
	rv, err := b.value(right, m)
	if err != nil {
		return nil, err
	}

	var out Expr
	if comparison {
		out, err = compare(cmpOp, lv, rv)
	} else {
		out, err = binaryOperators[operator](lv, rv)
	}
	if err != nil {
		return nil, err
	}

	return negate(out, negated), nil
}

// membershipOverRelation lowers `<value> in R.attr.<collection>` where the collection is stored in
// a related table.
func (b *builder) membershipOverRelation(needle *node, rel *Relation, parent string, m Mapper, negated bool) (Expr, error) {
	if rel.Field == nil {
		return nil, fmt.Errorf(
			"membership against a relation requires the relation to map its element column " +
				"(set Field on the relation)",
		)
	}

	needleValue, err := b.value(needle, m)
	if err != nil {
		return nil, err
	}

	alias := b.newAlias()
	elementCol := elementColumn(alias, rel.Field)

	omitted := b.opts.NullRepresentation == NullOmitted
	if needle.isVariable() {
		if entry, ok := m.Resolve(needle.variable); ok && entry.NullConvention != NullConventionUnset {
			omitted = entry.NullConvention == NullConventionOmitted
		}
	}
	nullSafe := !omitted || rel.Field.NullConvention == NullConventionExplicit
	body, err := b.elementMatches(elementCol, needleValue, nullSafe)
	if err != nil {
		return nil, err
	}

	// Membership is an `exists` fold, so it carries the same three-valued semantics: a NULL
	// element makes the comparison UNKNOWN, which must stay UNKNOWN rather than decaying to
	// false — otherwise `!(x in tagNames)` would allow a row the PDP denies.
	result := b.triStateExists(rel, alias, parent, body, existsSemantics, negated)
	if needleExpr, ok := needleValue.(Expr); ok && omitted {
		return Case{Whens: []When{{Cond: IsNull{X: needleExpr, Negate: true}, Then: result}}}, nil
	}
	return result, nil
}

// elementColumn is a relation's element column as seen from inside its subquery.
//
// It keeps the element's declared type so a literal of another type is answered as CEL answers
// it — `"2" in [2]` is false — rather than handed to an engine that coerces '2' onto a numeric
// column, or 'true' onto a boolean one.
func elementColumn(alias string, field *Entry) Column {
	return Column{Qualifier: alias, Name: field.Column, Type: field.ValueType}
}

// elementMatches builds the per-element predicate for membership against a stored collection.
//
// The corpus sends collection projections such as `tagNames` under the EXPLICIT-null convention,
// so a NULL element is a real null member rather than a missing one and `null in tagNames` has to
// be true. SQL equality never matches two NULLs, so the both-null case is spelled out — and only
// when the needle is itself a column, since a literal null needle collapses to a plain IS NULL.
func (b *builder) elementMatches(element Column, needle value, nullSafe bool) (Expr, error) {
	if _, list := needle.([]any); list {
		return nil, fmt.Errorf("membership with a list-valued element cannot be represented by scalar SQL equality")
	}
	if needle == nil {
		return IsNull{X: element}, nil
	}

	needleExpr, isColumn := needle.(Column)
	if !isColumn {
		return compare(OpEq, element, needle)
	}

	// Collection elements are explicit values. The outer membership guard preserves
	// a missing needle before this definite per-element equality is evaluated.

	if !nullSafe {
		return Cmp{Op: OpEq, L: element, R: needleExpr}, nil
	}
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

// scope is the FROM list and correlation predicate of a subquery over a relation — everything
// every shape built on that relation shares, so each shape only has to say what it asks of it.
type scope struct {
	correlate Expr
	from      []FromItem
}

// exists is `EXISTS (SELECT 1 FROM <scope> WHERE <correlate> AND where)`.
func (s scope) exists(where Expr) Subquery {
	return Subquery{Kind: SubqueryExists, From: s.from, Correlate: s.correlate, Where: where}
}

// count is `(SELECT COUNT(*) FROM <scope> WHERE <correlate> AND where)`; a nil where counts every
// correlated row.
func (s scope) count(where Expr) Subquery {
	return Subquery{Kind: SubqueryCount, From: s.from, Correlate: s.correlate, Where: where}
}

// relationScope resolves a relation into the scope of a subquery over it. Intermediate hops are
// joined inside the subquery; only the parent row correlates out.
func (b *builder) relationScope(rel *Relation, alias, parent string) scope {
	from := []FromItem{{Table: rel.Table, Alias: alias}}
	preds := appendRestrictions(nil, alias, rel.SubqueryFilter)
	return b.joinHops(rel, parent, alias, from, preds)
}

// joinHops appends rel.Via's intermediate tables to a subquery's FROM list, joining each to the
// table one step further in (inner), and finally correlates the outermost table with the parent
// row. An empty inner means there is no element table to join the first hop to — the hop-only
// chain hopsExist builds.
func (b *builder) joinHops(rel *Relation, parent, inner string, from []FromItem, preds []Expr) scope {
	for _, hop := range rel.Via {
		hopAlias := b.newAlias()
		from = append(from, FromItem{Table: hop.Table, Alias: hopAlias})
		if inner != "" {
			preds = append(preds, Cmp{
				Op: OpEq,
				L:  Column{Qualifier: inner, Name: hop.ChildColumn},
				R:  Column{Qualifier: hopAlias, Name: hop.JoinColumn},
			})
		}
		// A hop the application's own reads hide is a hop that does not exist as far as the
		// resource attributes are concerned, so every subquery over it carries the restriction.
		preds = appendRestrictions(preds, hopAlias, hop.SubqueryFilter)
		inner = hopAlias
	}

	preds = append(preds, Cmp{
		Op: OpEq,
		L:  Column{Qualifier: inner, Name: rel.TargetColumn},
		R:  Column{Qualifier: parent, Name: rel.SourceColumn},
	})

	return scope{from: from, correlate: and(preds...)}
}

// appendRestrictions lowers the caller-declared store-side predicates for one table into the
// conjunction that correlates its subquery.
//
// They go in beside the join rather than around the subquery so every shape built on the scope —
// the truth witnesses, the UNKNOWN witness, the counts, the hop guard — inherits them without
// each one having to remember to.
func appendRestrictions(preds []Expr, alias string, restrictions []Restriction) []Expr {
	for _, r := range restrictions {
		col := Column{Qualifier: alias, Name: r.Column}
		switch r.Op {
		case RestrictEq:
			preds = append(preds, Cmp{Op: OpEq, L: col, R: Lit{V: r.Value}})
		case RestrictNe:
			preds = append(preds, Cmp{Op: OpNe, L: col, R: Lit{V: r.Value}})
		case RestrictIsNull:
			preds = append(preds, IsNull{X: col})
		case RestrictIsNotNull:
			preds = append(preds, IsNull{X: col, Negate: true})
		case RestrictIn, RestrictNotIn:
			// CEL's own identities: membership in an empty list is false, so an empty IN hides
			// every row and an empty NOT IN hides none. Spelling them as constants also keeps
			// the renderer away from `IN ()`, which is a syntax error.
			if len(r.Values) == 0 {
				preds = append(preds, BoolConst{V: r.Op == RestrictNotIn})
				continue
			}
			vs := make([]Expr, 0, len(r.Values))
			for _, v := range r.Values {
				vs = append(vs, Lit{V: v})
			}
			in := Expr(InList{X: col, Vs: vs})
			if r.Op == RestrictNotIn {
				in = Not{X: in}
			}
			preds = append(preds, in)
		}
	}
	return preds
}

// hopsExist builds "every intermediate table of a flattened chain has a row", or nil when the
// relation is direct.
//
// CEL cannot dot through a list, so each `Via` hop of `mainCategory.subCategories` stands for a
// to-ONE parent: when it is absent the caller sends no attribute at all and CEL raises a
// missing-path error, which denies. A subquery rooted at the resource row cannot see the
// difference — an absent parent and a childless parent both return nothing — so `all` reads TRUE,
// `!exists` reads TRUE and the count reads 0, each admitting rows the PDP denies
// (cerbos/query-plan-adapters#309). Requiring the hops separately restores the distinction:
// guarded expressions become UNKNOWN, which excludes the row under BOTH polarities.
func (b *builder) hopsExist(rel *Relation, parent string) Expr {
	if len(rel.Via) == 0 {
		return nil
	}
	// The same hop joins and restrictions relationScope builds, minus the element table: hop i's
	// ChildColumn lives on the previous hop here, and the first hop has nothing further in.
	return b.joinHops(rel, parent, "", nil, nil).exists(nil)
}

// requireHops makes expr UNKNOWN unless every intermediate to-one hop exists. The CASE has no
// ELSE on purpose: a missing hop yields NULL, and NOT NULL is still NULL.
func (b *builder) requireHops(rel *Relation, parent string, expr Expr) Expr {
	guard := b.hopsExist(rel, parent)
	if guard == nil {
		return expr
	}
	return Case{Whens: []When{{Cond: guard, Then: expr}}}
}

// triStateExists builds the CASE that preserves CEL's three states across a correlated subquery.
func (b *builder) triStateExists(rel *Relation, alias, parent string, body Expr, sem macroSemantics, negated bool) Expr {
	s := b.relationScope(rel, alias, parent)

	triState := Case{
		Whens: []When{
			{Cond: s.exists(TruthTest{X: body, Want: sem.witness}), Then: BoolConst{V: sem.witnessResult}},
			{Cond: s.exists(TruthTest{X: body, Want: TruthUnknown}), Then: Lit{V: nil}},
		},
		Else: BoolConst{V: sem.defaultResult},
	}

	// An absent to-one parent must stay UNKNOWN rather than reaching the empty-collection
	// answer, which `all` reads as TRUE and `!exists` inverts into an allow (#309).
	return negate(b.requireHops(rel, parent, triState), negated)
}

// collectionMacro lowers exists/all/exists_one (and rejects filter/map).
func (b *builder) collectionMacro(n *node, m Mapper, negated bool) (Expr, error) {
	if len(n.operands) != binaryOperands {
		return nil, fmt.Errorf("'%s' requires exactly two operands", n.operator)
	}
	collection, lambda := n.operands[0], n.operands[1]

	// A literal value list arrives when the planner could not unroll a macro over a known
	// collection (more than 10 elements; cerbos/cerbos#2570, #2817). Fold it here so the
	// translation does not depend on which side of that threshold the collection landed.
	if collection.isValue() {
		return b.foldValueListMacro(n.operator, collection.value, lambda, m, negated)
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

	bodyExpr, err := b.predicate(body, inner, false)
	if err != nil {
		return nil, err
	}

	// Built before the switch even though exists and all build their own inside triStateExists:
	// building a scope numbers the aliases of a chain's hops, so moving this would renumber the
	// generated aliases in the emitted SQL.
	s := b.relationScope(rel, alias, entry.Qualifier)

	switch n.operator {
	case "exists":
		return b.triStateExists(rel, alias, entry.Qualifier, bodyExpr, existsSemantics, negated), nil

	case "all":
		return b.triStateExists(rel, alias, entry.Qualifier, bodyExpr, allSemantics, negated), nil

	case "exists_one":
		// exists_one never absorbs an erroring element, so the UNKNOWN witness is checked first
		// and only then is the exact-one count decided.
		count := s.count(TruthTest{X: bodyExpr, Want: TruthTrue})
		triState := Case{
			Whens: []When{
				{Cond: s.exists(TruthTest{X: bodyExpr, Want: TruthUnknown}), Then: Lit{V: nil}},
				{Cond: Cmp{Op: OpEq, L: count, R: Lit{V: float64(1)}}, Then: BoolConst{V: true}},
			},
			Else: BoolConst{V: false},
		}
		// An absent to-one parent must stay UNKNOWN here too, or `!exists_one(chain, ...)` reads
		// the empty tail as a determined false and inverts into an allow (#309).
		return negate(b.requireHops(rel, entry.Qualifier, triState), negated), nil

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
func (b *builder) foldValueListMacro(operator string, elements any, lambda *node, m Mapper, negated bool) (Expr, error) {
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

	parts := make([]Expr, 0, len(list))
	for _, element := range list {
		substituted, err := substituteLambdaVariable(body, variable, element)
		if err != nil {
			return nil, err
		}
		p, err := b.predicate(substituted, m, negated)
		if err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}

	// exists() is a disjunction of its bodies and all() a conjunction, swapped under negation
	// (De Morgan). An empty list needs no special case: or() and and() over no parts are CEL's
	// identities, so exists() matches nothing and all() matches everything.
	if (operator == "exists") != negated {
		return or(parts...), nil
	}
	return and(parts...), nil
}

// hasIntersection lowers `a.hasIntersection(b)`.
func (b *builder) hasIntersection(n *node, m Mapper, negated bool) (Expr, error) {
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
		body, err := membership(elementColumn(alias, rel.Field), list)
		if err != nil {
			return nil, err
		}
		return b.triStateExists(rel, alias, parent, body, existsSemantics, negated), nil
	}

	otherValue, err := b.value(otherNode, m)
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
				{Cond: deferred.exists(IsNull{X: deferred.body}), Then: Lit{V: nil}},
				{Cond: deferred.exists(TruthTest{X: member, Want: TruthTrue}), Then: BoolConst{V: true}},
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
	body Expr
	scope
	isMap bool
}

// macro names the CEL macro this collection came from, for error messages.
func (d deferredCollection) macro() string {
	if d.isMap {
		return "map"
	}
	return "filter"
}

func (b *builder) deferredCollection(n *node, m Mapper) (value, error) {
	if len(n.operands) != binaryOperands {
		return nil, fmt.Errorf("'%s' requires exactly two operands", n.operator)
	}
	collection, lambda := n.operands[0], n.operands[1]

	if !collection.isVariable() {
		return nil, fmt.Errorf("'%s' requires a collection attribute", n.operator)
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

	var bodyExpr Expr
	if n.operator == "map" {
		projected, err := b.value(body, inner)
		if err != nil {
			return nil, err
		}
		if bodyExpr, err = asExpr(projected); err != nil {
			return nil, err
		}
	} else if bodyExpr, err = b.predicate(body, inner, false); err != nil {
		return nil, err
	}

	return deferredCollection{
		scope: b.relationScope(rel, alias, entry.Qualifier), body: bodyExpr, isMap: n.operator == "map",
	}, nil
}

// value lowers a node in value position.
func (b *builder) value(n *node, m Mapper) (value, error) {
	switch {
	case n.isValue():
		return n.value, nil
	case n.isVariable():
		return b.resolveVariable(n.variable, m)
	}

	switch n.operator {
	case "and", "or", "not":
		return b.predicate(n, m, false)

	case "if":
		return b.ternaryValue(n, m)

	case "hierarchy":
		if len(n.operands) == 0 || len(n.operands) > binaryOperands {
			return nil, fmt.Errorf("'hierarchy' requires one or two operands")
		}
		target, err := b.value(n.operands[0], m)
		if err != nil {
			return nil, err
		}
		var delimiter value
		if len(n.operands) == binaryOperands {
			if delimiter, err = b.value(n.operands[1], m); err != nil {
				return nil, err
			}
		}
		return newHierarchy(target, delimiter)

	case "size":
		return b.size(n, m)

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
		v, err := b.value(operand, m)
		if err != nil {
			return nil, err
		}
		return parseTimestamp(v, temporal)

	case "double", "int":
		// SQL CAST is not a CEL conversion. CEL reads a WHOLE string or raises (and an error
		// denies the row), while CAST reads whatever numeric prefix parses: `CAST('100%_done' AS
		// INTEGER)` is 100 on SQLite and 0 on MySQL, so a direct lowering returns rows the PDP
		// denies. The numeric direction is no safer — CEL's int() truncates toward zero where
		// PostgreSQL and MySQL round to nearest, so int(-0.6) is 0 to CEL and -1 to them.
		// Nothing in the plan says what type the column holds, so fail closed instead
		// (cerbos/query-plan-adapters#311).
		return nil, fmt.Errorf(
			"'%s()' cannot be lowered to SQL CAST: CAST reads a numeric prefix where CEL requires "+
				"the whole string and raises otherwise, and PostgreSQL and MySQL round where CEL "+
				"truncates toward zero", n.operator,
		)

	case "string":
		if len(n.operands) != unaryOperands {
			return nil, fmt.Errorf("'%s' requires exactly one operand", n.operator)
		}
		v, err := b.value(n.operands[0], m)
		if err != nil {
			return nil, err
		}
		return castValue(v)
	}

	if _, ok := arithmeticOps[n.operator]; ok {
		return b.arithmetic(n, m)
	}

	if lambdaBinding[n.operator] {
		// A macro can appear in value position when it is the condition of a ternary.
		return b.collectionMacro(n, m, false)
	}

	// Anything else in value position is a predicate embedded in a value slot.
	return b.predicate(n, m, false)
}

func (b *builder) ternaryValue(n *node, m Mapper) (value, error) {
	if len(n.operands) != ternaryOperands {
		return nil, fmt.Errorf("'if' requires exactly three operands")
	}
	cond, err := b.predicate(n.operands[0], m, false)
	if err != nil {
		return nil, err
	}
	thenValue, err := b.value(n.operands[1], m)
	if err != nil {
		return nil, err
	}
	elseValue, err := b.value(n.operands[2], m)
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
func (b *builder) size(n *node, m Mapper) (value, error) {
	if len(n.operands) != unaryOperands {
		return nil, fmt.Errorf("'size' requires exactly one operand")
	}
	operand := n.operands[0]

	if operand.isVariable() {
		if entry, ok := m.Resolve(operand.variable); ok && entry.Relation != nil {
			// size() counts elements without evaluating them, so a NULL element column still
			// counts and no error guard is needed.
			count := b.relationScope(entry.Relation, b.newAlias(), entry.Qualifier).count(nil)
			// An absent to-one parent counts as UNKNOWN, not 0: `size(chain) == 0` and
			// `size(chain) >= 0` are both TRUE over an empty count and would return every
			// parentless row (#309).
			return b.requireHops(entry.Relation, entry.Qualifier, count), nil
		}
	}

	v, err := b.value(operand, m)
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
			Whens: []When{{Cond: deferred.exists(TruthTest{X: deferred.body, Want: TruthUnknown}), Then: Lit{V: nil}}},
			Else:  deferred.count(TruthTest{X: deferred.body, Want: TruthTrue}),
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
	if knownNonString(v) {
		return Lit{V: nil}, nil
	}
	return Call{Name: FuncCharLength, Args: []Expr{e}}, nil
}

func (b *builder) arithmetic(n *node, m Mapper) (value, error) {
	if len(n.operands) != binaryOperands {
		return nil, fmt.Errorf("'%s' requires exactly two operands", n.operator)
	}
	lv, err := b.value(n.operands[0], m)
	if err != nil {
		return nil, err
	}
	rv, err := b.value(n.operands[1], m)
	if err != nil {
		return nil, err
	}

	if n.operator == "div" {
		return floatDiv(lv, rv)
	}

	op := arithmeticOps[n.operator]

	// CEL overloads `+` on strings, and it reaches the wire as the same `add` node as numeric
	// addition. Which overload it is has to be read off the operands, because the plan never
	// says: CEL has no mixed-type `+`, so ONE string operand means the whole expression is a
	// concatenation. Only `add` has the overload — the other four over a string are a CEL
	// no-overload error, which denies, and they keep falling through to the numeric path.
	if op == OpAdd {
		return addValue(lv, rv)
	}

	return numericArith(op, lv, rv)
}

// numericArith lowers the numeric reading of a binary arithmetic operator.
func numericArith(op ArithOp, lv, rv value) (value, error) {
	// A retained ternary (a division that may be non-finite) must keep propagating symbolically
	// through the surrounding arithmetic rather than being lowered to SQL NULL.
	if out, ok, err := arithOverConditional(op, lv, rv); err != nil || ok {
		return out, err
	}

	// Fold constant arithmetic in double precision, matching CEL's number model exactly rather
	// than deferring to the dialect's numeric type.
	lf, lok := asFloat(lv)
	rf, rok := asFloat(rv)
	if lok && rok {
		folded, err := foldArithmetic(op, lf, rf)
		if err != nil {
			return nil, err
		}
		if folded != nil {
			return *folded, nil
		}
	}

	if op == OpMod && (isAttribute(lv) || isAttribute(rv)) {
		return nil, errors.New(
			"modulus over an attribute is a CEL no-overload error: Cerbos sends every attribute " +
				"number as a double and CEL's % is integer-only, so the PDP denies every row",
		)
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

// isAttribute reports whether an operand is an attribute read straight from storage — a column or a
// scalar read through a to-one hop — as opposed to a computed value such as size().
func isAttribute(v value) bool {
	switch t := v.(type) {
	case Column:
		return true
	case Subquery:
		return t.Kind == SubqueryScalar
	}
	return false
}

// foldArithmetic evaluates a binary operation over two constants, returning nil when the operator
// has no constant folding of its own (division stays symbolic so its IEEE cases survive).
func foldArithmetic(op ArithOp, l, r float64) (*float64, error) {
	var out float64
	switch op {
	case OpAdd:
		out = l + r
	case OpSub:
		out = l - r
	case OpMult:
		out = l * r
	case OpMod:
		// CEL's % is integer-only. Truncating a fractional operand here would be a silent
		// semantic change, and truncating a divisor in (-1, 1) to zero would panic outright, so
		// anything non-integral is rejected.
		if l != math.Trunc(l) || r != math.Trunc(r) {
			return nil, fmt.Errorf("modulus requires integer operands, got %v %% %v", l, r)
		}
		if r == 0 {
			return nil, fmt.Errorf("modulus by zero in query plan")
		}
		out = float64(int64(l) % int64(r))
	case OpDiv:
		// Routed to floatDiv above so the IEEE cases stay symbolic; named so a new operator
		// cannot silently fall through.
		return nil, nil
	}
	return &out, nil
}

// isStringOperand reports whether a translated operand is known to be a string: a string literal
// from the plan, or a column the caller declared ValueString.
func isStringOperand(v value) bool {
	if _, ok := v.(string); ok {
		return true
	}
	c, ok := v.(Column)
	return ok && c.Type == ValueString
}

// isUntypedColumn reports whether an operand is a bare column the caller declared no type for.
// Two of them on one `+` is the only shape whose overload cannot be resolved.
func isUntypedColumn(v value) bool {
	c, ok := v.(Column)
	return ok && c.Type != ValueString && c.Type != ValueBool
}

// addValue lowers CEL's `+`, choosing between numeric addition and string concatenation.
//
// One operand of a known type settles it, because CEL has no mixed-type `+`: a string literal or a
// column declared ValueString makes the whole expression a concatenation. Everything else keeps the
// numeric reading it has always had — including the retained ternaries and divisions that arrive as
// non-constant values — EXCEPT the one shape that reveals nothing at all, two undeclared columns.
//
// That shape fails closed. Guessing numeric is what this module used to do, and it is wrong in the
// dangerous direction: `text + text` is a hard error on PostgreSQL, 0 on SQLite, and 0 on MySQL,
// where comparing that 0 against a string constant coerces the CONSTANT to 0 as well and matches
// every row whose columns are non-NULL (cerbos/query-plan-adapters#391).
//
// There is no ValueNumber to pair with ValueString: nothing would reach it. Every numeric `add` the
// planner emits carries a constant operand — that is what makes it arithmetic rather than
// concatenation — so a declaration for the both-columns numeric case would be a constant no
// translation reads, the same reason there is no CastInt (#319). A corpus action that reaches it is
// what should introduce it.
func addValue(lv, rv value) (value, error) {
	if isStringOperand(lv) || isStringOperand(rv) {
		l, err := asExpr(lv)
		if err != nil {
			return nil, err
		}
		r, err := asExpr(rv)
		if err != nil {
			return nil, err
		}
		return Concat{L: l, R: r}, nil
	}

	if isUntypedColumn(lv) && isUntypedColumn(rv) {
		return nil, errors.New(
			"cannot tell numeric addition from string concatenation in `+` between two columns: " +
				"CEL overloads `+` on strings and the query plan carries no operand types, so " +
				"declare the string column's ValueType as ValueString",
		)
	}

	return numericArith(OpAdd, lv, rv)
}

// castValue lowers CEL's string() conversion. int() and double() are rejected before they reach
// here — SQL CAST does not reproduce their semantics (#311) — so string() is the only survivor.
//
// A text operand is cast as it stands. A column declared ValueNumber is not: CEL spells a double
// with Go's %g, which no engine's CAST prints, so it is held as numberText for the comparison that
// consumes it. A constant or undeclared operand is cast as it stands. A BOOLEAN operand cannot be —
// SQLite and MySQL have no boolean type and store 1/0, so `CAST(a_bool AS TEXT)` is '1' where CEL
// and PostgreSQL say 'true' (#376) — so boolOperand picks out every operand known to be boolean and
// it is spelled through boolText before it is cast.
func castValue(v value) (value, error) {
	e, err := asExpr(v)
	if err != nil {
		return nil, err
	}
	if _, constant := v.(float64); !constant && scalarKind(v) == "number" {
		return numberText{x: e}, nil
	}
	if boolOperand(e) {
		e = boolText(e)
	}
	return Cast{X: e, To: CastText}, nil
}

// boolOperand reports whether string()'s operand is known to be boolean. That is three shapes:
//
//   - a column declared ValueBool;
//   - a column declared ValueBool and read through a to-one hop, which is a scalar subquery
//     projecting it (cast/string/negated-from-boolean-through-relation);
//   - a predicate node, the lowering of a boolean-valued expression such as
//     `string(R.attr.n > 3)` (cast/string/negated-from-boolean-expression).
//
// Nothing in the plan names an attribute's type, so an undeclared column is not known to be
// boolean, through a hop or not, and keeps the plain CAST: declaring ValueBool is what tells the
// adapter a column holds a boolean (#470).
func boolOperand(e Expr) bool {
	switch t := e.(type) {
	case Column:
		return t.Type == ValueBool
	case Subquery:
		if t.Kind == SubqueryExists {
			return true
		}
		c, ok := t.Select.(Column)
		return t.Kind == SubqueryScalar && ok && c.Type == ValueBool
	case Cmp, Logic, Not, IsNull, TruthTest, Like, NotDistinct, InList, BoolConst:
		return true
	}
	return false
}

// boolText spells a boolean operand the way CEL's string() does:
//
//	CASE WHEN x IS NULL THEN NULL WHEN x THEN 'true' ELSE 'false' END
//
// A boolean column, a scalar subquery projecting one, and a predicate are all read as a condition
// by SQLite, MySQL and PostgreSQL alike — it is how a bare boolean conjunct already renders — so
// this one tree gives CEL's two words on every engine, where a CAST gives them on one
// (cerbos/query-plan-adapters#418, #470).
//
// The IS NULL arm is load-bearing. A NULL operand is a missing attribute, an absent hop or a null
// value, and CEL has no string() for any of them: it raises, and the PDP denies. `WHEN x` is
// UNKNOWN for a NULL operand, so without the arm the CASE would fall through to its ELSE and say
// 'false', and `string(x) != "true"` would return a row the PDP denies. With it the result is
// NULL, and the row stays out under both polarities.
//
// castValue still casts the result to text, so the renderer treats it exactly as it treats any
// other string(). On MySQL that cast is what gives the two words a byte-exact collation. A bare
// CASE compares in the connection's collation, which ignores case and trailing spaces by default:
// `string(x) == "TRUE"` and `== "true "` would both match a true row CEL rejects. The corpus case
// cast/string/from-boolean-case-changed-literal fails on MySQL without it.
func boolText(x Expr) Expr {
	return Case{
		Whens: []When{
			{Cond: IsNull{X: x}, Then: Lit{V: nil}},
			{Cond: x, Then: Lit{V: "true"}},
		},
		Else: Lit{V: "false"},
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
	if entry.ScalarRelation != nil {
		return b.scalarThroughHop(entry), nil
	}
	return Column{
		Qualifier:    entry.Qualifier,
		Name:         entry.Column,
		ExplicitNull: entry.NullConvention == NullConventionExplicit,
		Type:         entry.ValueType,
	}, nil
}

// scalarThroughHop reads one column of a to-ONE relation as a correlated scalar subquery.
//
// No hop guard is wrapped around it, unlike the collection chains: a subquery with no correlated
// row IS NULL, which is already CEL's missing-path error, and NULL propagates through every
// comparison and negation built on top of it. The guard the collection macros need exists because
// EXISTS is two-valued and collapses "absent parent" onto "no matching child"; a scalar projection
// never makes that collapse.
func (b *builder) scalarThroughHop(entry Entry) Expr {
	alias := b.newAlias()
	s := b.relationScope(entry.ScalarRelation, alias, entry.Qualifier)
	return Subquery{
		Kind:      SubqueryScalar,
		From:      s.from,
		Correlate: s.correlate,
		Select: Column{
			Qualifier:    alias,
			Name:         entry.Column,
			ExplicitNull: entry.NullConvention == NullConventionExplicit,
			Type:         entry.ValueType,
		},
	}
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
			return valueNode(element), nil
		}
		if rest, ok := strings.CutPrefix(n.variable, variable+"."); ok && rest != "" {
			current := element
			for _, segment := range strings.Split(rest, ".") {
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
			return valueNode(current), nil
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
func assertNoNullOperands(n *node, m Mapper, fallback NullRepresentation) error {
	if n.isValue() {
		if carriesNull(n.value) && fallback == NullOmitted {
			return errNullOperandUnderOmitted()
		}
		return nil
	}
	// A comparison between a mapped attribute and a literal is decided by that attribute's own
	// declaration, which is what lets one call carry both conventions. Confined to that shape: a
	// null buried in a macro over a literal list reaches a comparison long after this scan, and
	// nothing here can say which column it will land against, so those keep using the fallback.
	if variable, literal, ok := comparedAttributeAndLiteral(n); ok {
		if entry, found := m.Resolve(variable); found && entry.NullConvention != NullConventionUnset {
			if carriesNull(literal.value) && entry.NullConvention == NullConventionOmitted {
				return errNullOperandUnderOmitted()
			}
			return nil
		}
	}
	for _, child := range n.operands {
		if err := assertNoNullOperands(child, m, fallback); err != nil {
			return err
		}
	}
	return nil
}

func errNullOperandUnderOmitted() error {
	return fmt.Errorf(
		"cannot translate a null operand under NullOmitted: a NULL column sends no " +
			"attribute, so Cerbos evaluates the comparison as a missing-attribute error " +
			"(deny) while a NULL-selecting filter would return those rows. Send NULL " +
			"columns as explicit nulls and use NullExplicit, or keep this shape out of the policy",
	)
}

// comparedAttributeAndLiteral destructures a binary comparison between a plan variable and a
// literal, in either operand order.
func comparedAttributeAndLiteral(n *node) (string, *node, bool) {
	if !n.isExpr() || len(n.operands) != binaryOperands {
		return "", nil, false
	}
	if _, ok := comparisonOps[n.operator]; !ok && n.operator != "in" {
		return "", nil, false
	}
	left, right := n.operands[0], n.operands[1]
	variable, literal := left, right
	if right.isVariable() {
		variable, literal = right, left
	}
	if !variable.isVariable() || !literal.isValue() {
		return "", nil, false
	}
	return variable.variable, literal, true
}

// carriesNull reports whether a literal contains a null anywhere inside it.
//
// The recursion is load-bearing: a macro over a literal list of objects has its lambda body
// substituted with each element's fields, so a null nested in `[{"v": null}]` reaches a comparison
// as a bare null operand long after this scan has run.
func carriesNull(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case []any:
		for _, item := range t {
			if carriesNull(item) {
				return true
			}
		}
	case map[string]any:
		for _, item := range t {
			if carriesNull(item) {
				return true
			}
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
