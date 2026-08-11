// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package queryplan

// The SQL expression tree the translator produces. It carries no dialect syntax: nothing here
// decides how an identifier is quoted, how a parameter is spelled, or how a cast is written. Each
// module's own render.go owns all of that.
//
// The separation exists so the parts that are easy to get subtly wrong — operand order, escaping,
// three-valued logic — can be read and tested without wading through string building.
//
// This package is vendored byte-for-byte into both the ent and pgx modules, so that a consumer of
// either pulls in only the one. `conformance/scripts/validate-corpus.sh` diffs the two trees and
// fails on any difference: the same semantic fix has to land in both copies, and nothing else
// notices when it lands in only one.

// Expr is any node of the abstract expression tree. Expressions are either predicates (boolean
// results) or values; the translator tracks which is which by construction rather than by type,
// mirroring the reference adapters.
type Expr interface {
	isExpr()
}

// CmpOp is a comparison operator. These are the six CEL comparisons after value-first mirroring
// has been applied (see mirroredOperators in translate.go).
type CmpOp string

const (
	OpEq CmpOp = "eq"
	OpNe CmpOp = "ne"
	OpLt CmpOp = "lt"
	OpLe CmpOp = "le"
	OpGt CmpOp = "gt"
	OpGe CmpOp = "ge"
)

// Mirror returns the operator that means the same thing with the operands swapped. Equality is
// symmetric; the four ordered comparisons flip.
func (o CmpOp) Mirror() CmpOp {
	switch o {
	case OpLt:
		return OpGt
	case OpGt:
		return OpLt
	case OpLe:
		return OpGe
	case OpGe:
		return OpLe
	default:
		return o
	}
}

// ArithOp is a binary arithmetic operator.
type ArithOp string

const (
	OpAdd  ArithOp = "add"
	OpSub  ArithOp = "sub"
	OpMult ArithOp = "mult"
	OpDiv  ArithOp = "div"
	OpMod  ArithOp = "mod"
)

// CastType is an abstract target type for a CEL type conversion.
type CastType string

// There is deliberately no integer cast: CEL's int() fails closed rather than lowering to SQL
// CAST (see the "double", "int" arm in translate.go), so a CastInt would be a constant no
// translation reaches and a render path no test could reach either
// (cerbos/query-plan-adapters#319). Re-introducing it belongs with the corpus action that drives
// it and the caller-declared numeric ValueType it needs.
const (
	CastText  CastType = "text"
	CastFloat CastType = "float"
)

// Column references a mapped column, optionally qualified by a table or subquery alias.
type Column struct {
	Qualifier string
	Name      string
	// ExplicitNull marks a column whose NULL the caller sends to check() as an explicit null
	// attribute, so CEL compares a null VALUE rather than raising a missing-attribute error. The
	// equality family has to render definitely for such a column; see Entry.NullConvention.
	ExplicitNull bool
	// IsBool marks a column the caller declared as boolean-typed, which is what lets `string()`
	// over it fail closed. Nothing in the plan names an operand's type, and a boolean is the one
	// type whose text rendering differs across the engines this module targets; see
	// Entry.ValueType and castValue.
	IsBool bool
}

// Lit is a value bound as a query parameter. A nil V renders as SQL NULL.
type Lit struct {
	V any
}

// BoolConst is a constant predicate — the always-true and always-false filters, and the identity
// results of a macro folded over an empty collection.
type BoolConst struct {
	V bool
}

// Cmp is a binary comparison. Operands arrive already normalised: any value-first mirroring has
// been applied by the translator, so Cmp is read exactly as written.
type Cmp struct {
	L  Expr
	R  Expr
	Op CmpOp
}

// Arith is a binary arithmetic expression producing a value.
type Arith struct {
	L  Expr
	R  Expr
	Op ArithOp
}

// Concat is CEL's `+` over strings, which is a different operator from Arith{Op: OpAdd} in every
// engine even though the plan spells them the same. It is a node of its own rather than a sixth
// ArithOp because the two are not interchangeable anywhere: the SQL spelling differs per dialect
// (`||` against `CONCAT`), and rendering a string concatenation as `+` is silently wrong rather
// than a syntax error on MySQL, which coerces both operands to 0 and matches rows the policy never
// allowed (cerbos/query-plan-adapters#376).
type Concat struct {
	L Expr
	R Expr
}

// Logic is an n-ary AND/OR over predicates.
type Logic struct {
	Xs  []Expr
	And bool
}

// Not negates a predicate. SQL's three-valued logic is load-bearing here: NOT UNKNOWN is UNKNOWN,
// which is what keeps a row with a NULL operand excluded under both polarities and matches CEL
// treating a missing attribute as an evaluation error (a deny).
type Not struct {
	X Expr
}

// IsNull tests a value against SQL NULL.
type IsNull struct {
	X      Expr
	Negate bool
}

// TruthValue is the state a TruthTest asks about.
type TruthValue uint8

const (
	// TruthTrue is `IS TRUE`.
	TruthTrue TruthValue = iota
	// TruthFalse is `IS FALSE`.
	TruthFalse
	// TruthUnknown is `IS NULL` applied to a predicate rather than a value.
	TruthUnknown
)

// TruthTest collapses SQL's three-valued logic to two values so a macro can distinguish its
// witnesses. It is what lets a collection macro keep a CEL evaluation error as UNKNOWN instead of
// letting it decay into FALSE — the difference between `!tags.exists(...)` denying a row (correct)
// and allowing it (an authorization bug).
type TruthTest struct {
	X    Expr
	Want TruthValue
}

// Like is a pattern match with an explicit escape character. Metacharacters in the needle are
// always escaped before they reach here (see escapeLikeLiteral / escapeLikeColumn), so a `%` in
// policy data can never act as a wildcard.
type Like struct {
	Receiver Expr
	Pattern  Expr
}

// NotDistinct is null-safe equality (`IS NOT DISTINCT FROM`).
//
// It is what membership against an explicit-null collection needs: under that convention a null
// element is a real value, so CEL compares `"x" == null` to FALSE rather than raising an error.
// Ordinary `=` would yield UNKNOWN there, which survives a negation and would wrongly exclude the
// row from `!(x in coll)`.
type NotDistinct struct {
	L Expr
	R Expr
}

// InList is membership against an explicit list of values.
type InList struct {
	X  Expr
	Vs []Expr
}

// When is one arm of a Case.
type When struct {
	Cond Expr
	Then Expr
}

// Case is a searched CASE. A nil Else yields SQL NULL, which is the whole point for the ternary
// and division shapes: an UNKNOWN condition must produce NULL rather than falling through to the
// else-branch, so the row stays excluded under both polarities.
type Case struct {
	Else  Expr
	Whens []When
}

// FuncName is an abstract function the renderer spells for its dialect.
type FuncName string

const (
	// FuncCharLength is the character (not byte) length of a string.
	FuncCharLength FuncName = "charLength"
	// FuncReplace is three-argument string replacement, used to escape LIKE metacharacters in a
	// column-valued needle at query time.
	FuncReplace FuncName = "replace"
	// FuncConcat joins strings. A NULL argument must propagate to a NULL result.
	FuncConcat FuncName = "concat"
	// FuncNullIf guards the finite arm of a division so dialects that evaluate CASE arms eagerly
	// do not abort the query on a division by zero that can never be selected.
	FuncNullIf FuncName = "nullIf"
)

// Call is a dialect function application.
type Call struct {
	Name FuncName
	Args []Expr
}

// Cast is a CEL type conversion.
type Cast struct {
	X  Expr
	To CastType
}

// SubqueryKind distinguishes the shapes a relation mapping lowers into.
type SubqueryKind uint8

const (
	// SubqueryExists tests whether any correlated row satisfies Where.
	SubqueryExists SubqueryKind = iota
	// SubqueryCount produces the number of correlated rows as a scalar value, which is what
	// `size(R.attr.tags)` and the relation-count thresholds need.
	SubqueryCount
	// SubqueryScalar reads Select from the single correlated row of a to-ONE relation, which is
	// what a scalar reached THROUGH a hop needs (`R.attr.parent.aString`).
	//
	// It is the one kind that can produce SQL NULL for a structural reason rather than a stored
	// one: no correlated row at all yields NULL, and that is exactly right. An absent to-one hop
	// sends no attribute, CEL raises a missing-path error and the PDP denies — and because NULL
	// propagates, `NOT (subquery = TRUE)` is NULL too, so the row stays excluded under BOTH
	// polarities without a separate hop guard (cerbos/query-plan-adapters#375).
	SubqueryScalar
)

// FromItem is one aliased table in a subquery's FROM clause.
type FromItem struct {
	Table string
	Alias string
}

// Subquery is a correlated subquery over one or more related tables.
//
// From carries the element table first and any intermediate hops after it, so a two-hop chain
// such as `mainCategory.subCategories` joins through its intermediate table inside the subquery
// while only the root row correlates outwards. Correlate is the full conjunction of hop joins and
// the outward correlation; Where is the translated lambda body, nil when the macro has no body
// (a bare size()).
type Subquery struct {
	Correlate Expr
	Where     Expr
	// Select is the projected expression, set only for SubqueryScalar and nil otherwise —
	// EXISTS projects a literal and COUNT projects the aggregate.
	Select Expr
	From   []FromItem
	Kind   SubqueryKind
}

func (Column) isExpr()      {}
func (Lit) isExpr()         {}
func (BoolConst) isExpr()   {}
func (Cmp) isExpr()         {}
func (Arith) isExpr()       {}
func (Concat) isExpr()      {}
func (Logic) isExpr()       {}
func (Not) isExpr()         {}
func (IsNull) isExpr()      {}
func (TruthTest) isExpr()   {}
func (Like) isExpr()        {}
func (NotDistinct) isExpr() {}
func (InList) isExpr()      {}
func (Case) isExpr()        {}
func (Call) isExpr()        {}
func (Cast) isExpr()        {}
func (Subquery) isExpr()    {}

// and combines predicates, flattening the single-operand case so the rendered SQL stays readable.
func and(xs ...Expr) Expr {
	return combine(true, xs)
}

func or(xs ...Expr) Expr {
	return combine(false, xs)
}

func combine(isAnd bool, xs []Expr) Expr {
	switch len(xs) {
	case 0:
		// CEL identity: an empty conjunction is true, an empty disjunction is false.
		return BoolConst{V: isAnd}
	case 1:
		return xs[0]
	default:
		return Logic{And: isAnd, Xs: xs}
	}
}
