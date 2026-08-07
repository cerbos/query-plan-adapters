// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

// Package cerbosent translates a Cerbos query plan into an ent predicate, ready to hand to a
// generated ent query through its `Where(func(*sql.Selector))` escape hatch.
//
// The invariant this adapter holds to is that a plan shape it cannot express exactly must return
// an error, never a best-effort predicate. A wrong filter is an authorization bug that returns
// rows the PDP denies; an error is a bug report. See ../conformance/README.md.
package cerbosent

import (
	"errors"
	"fmt"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"
	responsev1 "github.com/cerbos/cerbos/api/genpb/cerbos/response/v1"

	"github.com/cerbos/query-plan-adapters/ent/internal/queryplan"
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
	// then selects exactly the rows check() allows. This is the default.
	NullExplicit = queryplan.NullExplicit
	// NullOmitted — a NULL column sends no attribute, so CEL raises a missing-attribute error
	// (a deny) and a NULL-selecting filter would over-grant. Null operands are rejected.
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
	// NullConventionOmitted — this column's NULL sends no attribute, so null operands against it
	// are rejected whatever NullRepresentation says.
	NullConventionOmitted = queryplan.NullConventionOmitted
)

// PlanKind mirrors the plan's filter kind.
type PlanKind int

const (
	// KindAlwaysDenied means no row is accessible; the caller should skip the query entirely.
	KindAlwaysDenied PlanKind = iota
	// KindAlwaysAllowed means every row is accessible; the caller should apply no filter.
	KindAlwaysAllowed
	// KindConditional means Predicate carries the filter to apply.
	KindConditional
)

// Result is a translated plan.
type Result struct {
	// Predicate is the filter to apply, set only for KindConditional. It is an ordinary ent
	// predicate: pass it to a selector with `s.Where(result.Predicate)`.
	Predicate *sql.Predicate
	Kind      PlanKind
}

// ErrUnsupported wraps every shape this adapter refuses to translate, so callers can distinguish
// "the policy asks for something SQL cannot express" from a mapping or transport error.
var ErrUnsupported = errors.New("cerbosent: unsupported query plan shape")

type options struct {
	dialect            string
	nullRepresentation NullRepresentation
}

// Option configures a translation.
type Option func(*options)

// WithDialect declares which SQL dialect the predicate will be rendered for. It must match the
// dialect of the ent client the predicate is handed to, since cast spellings and timestamp storage
// differ between them. Defaults to dialect.SQLite, matching ent's own default.
func WithDialect(d string) Option {
	return func(o *options) { o.dialect = d }
}

// WithNullRepresentation declares how the caller represents a NULL column in the attributes it
// sends to check(). Defaults to NullExplicit.
func WithNullRepresentation(rep NullRepresentation) Option {
	return func(o *options) { o.nullRepresentation = rep }
}

// Translate lowers a PlanResources response into an ent predicate over table.
//
// table is the resource table's name — the same name ent generates for the entity being filtered
// — and is used to qualify its columns so the predicate composes correctly inside the correlated
// subqueries that collection attributes lower into.
//
// mapper resolves the plan's attribute references — e.g. `request.resource.attr.aString` — onto
// columns and relations. Resolution is fail-closed: an unmapped reference is an error rather than
// a guessed column name.
func Translate(plan *responsev1.PlanResourcesResponse, table string, mapper Mapper, opts ...Option) (Result, error) {
	if plan == nil {
		return Result{}, fmt.Errorf("cerbosent: nil plan response")
	}
	if table == "" {
		return Result{}, fmt.Errorf("cerbosent: table name is required")
	}
	if mapper == nil {
		return Result{}, fmt.Errorf("cerbosent: mapper is required")
	}

	cfg := options{dialect: dialect.SQLite}
	for _, opt := range opts {
		opt(&cfg)
	}

	filter := plan.GetFilter()
	if filter == nil {
		return Result{Kind: KindAlwaysDenied}, nil
	}

	switch filter.GetKind() {
	case enginev1.PlanResourcesFilter_KIND_ALWAYS_DENIED:
		return Result{Kind: KindAlwaysDenied}, nil
	case enginev1.PlanResourcesFilter_KIND_ALWAYS_ALLOWED:
		return Result{Kind: KindAlwaysAllowed}, nil
	case enginev1.PlanResourcesFilter_KIND_CONDITIONAL:
	default:
		return Result{}, fmt.Errorf("cerbosent: unrecognised filter kind %s", filter.GetKind())
	}

	expr, err := queryplan.Build(filter.GetCondition(), mapper, queryplan.Options{
		RootTable:          table,
		NullRepresentation: cfg.nullRepresentation,
	})
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrUnsupported, err)
	}

	predicate, err := render(expr, cfg.dialect)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrUnsupported, err)
	}

	return Result{Kind: KindConditional, Predicate: predicate}, nil
}
