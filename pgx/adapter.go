// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

// Package cerbospgx translates a Cerbos query plan into a PostgreSQL WHERE fragment and its bound
// arguments, ready to hand to pgx.
//
// The invariant this adapter holds to is that a plan shape it cannot express exactly must return
// an error, never a best-effort filter. A wrong filter is an authorization bug that returns rows
// the PDP denies; an error is a bug report. See ../conformance/README.md.
package cerbospgx

import (
	"errors"
	"fmt"

	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"
	responsev1 "github.com/cerbos/cerbos/api/genpb/cerbos/response/v1"

	"github.com/cerbos/query-plan-adapters/pgx/internal/queryplan"
)

// Mapping types describing how a plan's attribute references reach columns and related tables.
// They are aliased from the module's internal translator so that this package stays the single
// import a consumer needs.
type (
	// Entry is what a plan variable resolves to: a column, or a relation reaching another table.
	Entry = queryplan.Entry
	// Relation describes how a collection- or object-valued attribute reaches another table.
	Relation = queryplan.Relation
	// Hop is one intermediate table in a flattened relation chain.
	Hop = queryplan.Hop
	// Restriction is one caller-declared predicate on the rows a relation subquery may see.
	// See Relation.SubqueryFilter and "Mapping hazards" in the README.
	Restriction = queryplan.Restriction
	// RestrictOp is the comparison a Restriction applies.
	RestrictOp = queryplan.RestrictOp
	// Mapper resolves plan variables to storage.
	Mapper = queryplan.Mapper
	// MapperMap is a static reference-to-entry table.
	MapperMap = queryplan.MapperMap
	// MapperFunc adapts a function to Mapper.
	MapperFunc = queryplan.MapperFunc
	// NullRepresentation declares the caller's NULL-attribute convention.
	NullRepresentation = queryplan.NullRepresentation
	// NullConvention declares one attribute's NULL representation, overriding
	// NullRepresentation for that attribute.
	NullConvention = queryplan.NullConvention
)

// Value types.
const (
	ValueDefault   = queryplan.ValueDefault
	ValueTimestamp = queryplan.ValueTimestamp
	ValueBool      = queryplan.ValueBool
	ValueString    = queryplan.ValueString
	ValueNumber    = queryplan.ValueNumber
)

// Restriction comparisons, for Relation.SubqueryFilter and Hop.SubqueryFilter.
const (
	RestrictEq        = queryplan.RestrictEq
	RestrictNe        = queryplan.RestrictNe
	RestrictIsNull    = queryplan.RestrictIsNull
	RestrictIsNotNull = queryplan.RestrictIsNotNull
	RestrictIn        = queryplan.RestrictIn
	RestrictNotIn     = queryplan.RestrictNotIn
)

// NULL-attribute representations.
//
// The planner emits the same `eq(attr, null)` node under both conventions, so the plan cannot
// reveal which one the caller uses and the adapter has to be told. See
// https://github.com/cerbos/query-plan-adapters/issues/302.
const (
	// NullExplicit — a NULL column is sent to check() as an explicit null attribute. IS NULL
	// then selects exactly the rows check() allows.
	NullExplicit = queryplan.NullExplicit
	// NullOmitted — a NULL column sends no attribute, so CEL raises a missing-attribute error
	// (a deny) and a NULL-selecting filter would over-grant. Null operands are rejected. This is
	// the default: its wrong guess is a refusal, where NullExplicit's is an over-grant
	// (docs/adr/0011-denial-fails-closed-in-the-result.md).
	NullOmitted = queryplan.NullOmitted
)

// Per-attribute NULL conventions, set on Entry.NullConvention.
//
// One policy suite can legitimately mix the two — the same column mapped twice, sent as an
// explicit null under one attribute name and omitted under another — which the call-level
// NullRepresentation cannot express. See
// https://github.com/cerbos/query-plan-adapters/issues/308.
const (
	// NullConventionUnset declares nothing: the column is treated as NOT NULL when rendering a
	// comparison, and NullRepresentation still governs null-operand rejection.
	NullConventionUnset = queryplan.NullConventionUnset
	// NullConventionExplicit — this column's NULL is sent as an explicit null attribute, so the
	// equality family renders definitely and a negation includes the NULL rows CEL allows.
	NullConventionExplicit = queryplan.NullConventionExplicit
	// NullConventionOmitted — this column's NULL sends no attribute. `x == null` and `x != null`
	// render as UNKNOWN for a NULL column, as CEL's missing-attribute error; every other null
	// operand against it is rejected, whatever NullRepresentation says.
	NullConventionOmitted = queryplan.NullConventionOmitted
)

// PlanKind mirrors the plan's filter kind.
type PlanKind int

const (
	// KindAlwaysDenied means no row is accessible. Where is FALSE; the caller may skip the query
	// entirely.
	KindAlwaysDenied PlanKind = iota
	// KindAlwaysAllowed means every row is accessible. Where is TRUE.
	KindAlwaysAllowed
	// KindConditional means Where and Args carry the translated condition.
	KindConditional
)

// Result is a translated plan.
type Result struct {
	// Where is the filter to apply, for every Kind: a PostgreSQL boolean expression whose
	// placeholders are numbered from $1, or from $n+1 under WithPlaceholderOffset(n). It is never
	// empty on a successful translation, so a caller that splices it without switching on Kind
	// still gets exactly the rows the PDP allows
	// (docs/adr/0011-denial-fails-closed-in-the-result.md).
	Where string
	// Args binds Where's placeholders, in order.
	Args []any
	Kind PlanKind
}

var alwaysDenied = Result{Kind: KindAlwaysDenied, Where: "FALSE"}

// ErrUnsupported wraps every shape this adapter refuses to translate, so callers can distinguish
// "the policy asks for something PostgreSQL cannot express" from a mapping or transport error.
var ErrUnsupported = errors.New("cerbospgx: unsupported query plan shape")

type options struct {
	nullRepresentation NullRepresentation
	placeholderOffset  int
}

// Option configures a translation.
type Option func(*options)

// WithNullRepresentation declares how the caller represents a NULL column in the attributes it
// sends to check(). Defaults to NullOmitted.
func WithNullRepresentation(rep NullRepresentation) Option {
	return func(o *options) { o.nullRepresentation = rep }
}

// WithPlaceholderOffset shifts the generated `$n` placeholders so the fragment can be appended to
// a query that already binds n arguments.
func WithPlaceholderOffset(n int) Option {
	return func(o *options) { o.placeholderOffset = n }
}

// Translate lowers a PlanResources response into a PostgreSQL filter over table.
//
// mapper resolves the plan's attribute references — e.g. `request.resource.attr.aString` — onto
// columns and relations. Resolution is fail-closed: an unmapped reference is an error rather than
// a guessed column name.
func Translate(plan *responsev1.PlanResourcesResponse, table string, mapper Mapper, opts ...Option) (Result, error) {
	if plan == nil {
		return Result{}, fmt.Errorf("cerbospgx: nil plan response")
	}
	if table == "" {
		return Result{}, fmt.Errorf("cerbospgx: table name is required")
	}
	if mapper == nil {
		return Result{}, fmt.Errorf("cerbospgx: mapper is required")
	}

	cfg := options{nullRepresentation: NullOmitted}
	for _, opt := range opts {
		opt(&cfg)
	}

	filter := plan.GetFilter()
	if filter == nil {
		return alwaysDenied, nil
	}

	switch filter.GetKind() {
	case enginev1.PlanResourcesFilter_KIND_ALWAYS_DENIED:
		return alwaysDenied, nil
	case enginev1.PlanResourcesFilter_KIND_ALWAYS_ALLOWED:
		return Result{Kind: KindAlwaysAllowed, Where: "TRUE"}, nil
	case enginev1.PlanResourcesFilter_KIND_CONDITIONAL:
	default:
		return Result{}, fmt.Errorf("cerbospgx: unrecognised filter kind %s", filter.GetKind())
	}

	expr, err := queryplan.Build(filter.GetCondition(), mapper, queryplan.Options{
		RootTable:          table,
		NullRepresentation: cfg.nullRepresentation,
	})
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrUnsupported, err)
	}

	where, args, err := render(expr, cfg.placeholderOffset)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrUnsupported, err)
	}

	return Result{Kind: KindConditional, Where: where, Args: args}, nil
}
