// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

// Package rewrite applies Cerbos query plans to a SQL statement by replacing every read of a
// protected table with a derived table that holds only the rows the plan allows.
package rewrite

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"
	responsev1 "github.com/cerbos/cerbos/api/genpb/cerbos/response/v1"

	"github.com/cerbos/query-plan-adapters/sqlrewrite/pkg/rewrite/internal/queryplan"
)

type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectMySQL    Dialect = "mysql"
	DialectSQLite   Dialect = "sqlite"
)

type Resource struct {
	Attributes map[string]Entry `yaml:"attributes" json:"attributes"`
	Table      string           `yaml:"table" json:"table"`
}

type Config struct {
	Resources          map[string]Resource `yaml:"resources" json:"resources"`
	Dialect            Dialect             `yaml:"dialect" json:"dialect"`
	NullRepresentation NullRepresentation  `yaml:"nullRepresentation" json:"nullRepresentation"`
	Protect            []string            `yaml:"protect" json:"protect,omitempty"`
	Readers            []string            `yaml:"readers" json:"readers,omitempty"`
}

type Request struct {
	Plans  map[string]*responsev1.PlanResourcesResponse
	SQL    string
	Params []any
}

type Applied struct {
	Resource   string
	Table      string
	Kind       PlanKind
	References int
}

type Result struct {
	SQL     string
	Params  []any
	Applied []Applied
}

type PlanKind string

const (
	KindAlwaysAllowed PlanKind = "ALWAYS_ALLOWED"
	KindAlwaysDenied  PlanKind = "ALWAYS_DENIED"
	KindConditional   PlanKind = "CONDITIONAL"
)

type Code string

const (
	CodeUnsupportedPlan    Code = "unsupported_plan"
	CodeUnsupportedSQL     Code = "unsupported_sql"
	CodeUnmappedAttribute  Code = "unmapped_attribute"
	CodeMissingPlan        Code = "missing_plan"
	CodeUnusedPlan         Code = "unused_plan"
	CodeAmbiguousReference Code = "ambiguous_reference"
	CodeInvalidRequest     Code = "invalid_request"
)

var ErrRefused = errors.New("sqlrewrite: refused")

type RefusalError struct {
	Code    Code
	Message string
}

func (e *RefusalError) Error() string {
	return fmt.Sprintf("sqlrewrite: %s: %s", e.Code, e.Message)
}

func (e *RefusalError) Is(target error) bool {
	return target == ErrRefused
}

func refuse(code Code, format string, args ...any) error {
	return &RefusalError{Code: code, Message: fmt.Sprintf(format, args...)}
}

type resource struct {
	mapper queryplan.MapperMap
	kind   string
	name   tableName
}

type tableName struct {
	schema string
	name   string
}

func parseTableName(s string) (tableName, error) {
	parts := strings.Split(s, ".")
	switch len(parts) {
	case 1:
		if parts[0] != "" {
			return tableName{name: parts[0]}, nil
		}
	case 2: //nolint:mnd
		if parts[0] != "" && parts[1] != "" {
			return tableName{schema: parts[0], name: parts[1]}, nil
		}
	}
	return tableName{}, fmt.Errorf("table %q must be name or schema.name", s)
}

func (t tableName) String() string {
	if t.schema == "" {
		return t.name
	}
	return t.schema + "." + t.name
}

func (t tableName) matches(schema, name string) bool {
	if !strings.EqualFold(t.name, name) {
		return false
	}
	return schema == "" || t.schema == "" || strings.EqualFold(t.schema, schema)
}

type Rewriter struct {
	dialect   dialectRewriter
	name      Dialect
	resources []resource
	protected []tableName
	readers   []tableName
	nullRep   queryplan.NullRepresentation
}

type dialectRewriter interface {
	entDialect() string
	rewrite(*rewriteJob) (string, []any, error)
}

func New(conf Config) (*Rewriter, error) {
	r := &Rewriter{name: conf.Dialect}
	switch conf.Dialect {
	case DialectPostgres:
		r.dialect = postgresDialect{}
	case DialectMySQL:
		r.dialect = mysqlDialect{}
	case DialectSQLite:
		r.dialect = sqliteDialect{}
	default:
		return nil, fmt.Errorf("unknown dialect %q: expected postgres, mysql or sqlite", conf.Dialect)
	}

	var err error
	if r.nullRep, err = compileNullRepresentation(conf.NullRepresentation); err != nil {
		return nil, err
	}

	kinds := make([]string, 0, len(conf.Resources))
	for kind := range conf.Resources {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		res := conf.Resources[kind]
		name, err := parseTableName(res.Table)
		if err != nil {
			return nil, fmt.Errorf("resource %q: %w", kind, err)
		}
		mapper, err := compileMapper(res.Attributes)
		if err != nil {
			return nil, fmt.Errorf("resource %q: %w", kind, err)
		}
		for _, other := range r.resources {
			if other.name.matches(name.schema, name.name) {
				return nil, fmt.Errorf("resources %q and %q map the same table %s", other.kind, kind, name)
			}
		}
		r.resources = append(r.resources, resource{kind: kind, name: name, mapper: mapper})
		r.protected = append(r.protected, name)
	}
	for _, p := range conf.Protect {
		name, err := parseTableName(p)
		if err != nil {
			return nil, fmt.Errorf("protect: %w", err)
		}
		r.protected = append(r.protected, name)
	}
	for _, p := range conf.Readers {
		name, err := parseTableName(p)
		if err != nil {
			return nil, fmt.Errorf("readers: %w", err)
		}
		r.readers = append(r.readers, name)
	}
	return r, nil
}

func (r *Rewriter) Rewrite(ctx context.Context, req Request) (*Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.SQL) == "" {
		return nil, refuse(CodeInvalidRequest, "sql is empty")
	}
	if len(req.Plans) == 0 {
		return nil, refuse(CodeInvalidRequest, "no plans were sent")
	}
	for kind, plan := range req.Plans {
		if plan == nil {
			return nil, refuse(CodeInvalidRequest, "plan for %q is null", kind)
		}
		if _, ok := r.resourceByKind(kind); !ok {
			return nil, refuse(CodeInvalidRequest, "no resource %q is configured", kind)
		}
	}

	if strings.Contains(strings.ToLower(req.SQL), filterMarkerPrefix) {
		return nil, refuse(CodeUnsupportedSQL, "identifiers starting %s are reserved", filterMarkerPrefix)
	}

	job := &rewriteJob{rewriter: r, sql: req.SQL, plans: req.Plans, params: req.Params, wrappers: map[string]*wrapper{}}
	sql, params, err := r.dialect.rewrite(job)
	if err != nil {
		return nil, err
	}

	applied := make([]Applied, 0, len(job.wrappers))
	for kind := range req.Plans {
		w, ok := job.wrappers[kind]
		if !ok {
			return nil, refuse(CodeUnusedPlan, "a plan was sent for %q but the statement never reads %s", kind, r.mustResource(kind).name)
		}
		applied = append(applied, Applied{Resource: kind, Table: w.res.name.String(), Kind: w.kind, References: w.references})
	}
	sort.Slice(applied, func(i, j int) bool { return applied[i].Resource < applied[j].Resource })
	return &Result{SQL: sql, Params: params, Applied: applied}, nil
}

func (r *Rewriter) Dialect() Dialect { return r.name }

func (r *Rewriter) resourceByKind(kind string) (resource, bool) {
	for _, res := range r.resources {
		if res.kind == kind {
			return res, true
		}
	}
	return resource{}, false
}

func (r *Rewriter) mustResource(kind string) resource {
	res, _ := r.resourceByKind(kind)
	return res
}

type rewriteJob struct {
	rewriter *Rewriter
	plans    map[string]*responsev1.PlanResourcesResponse
	wrappers map[string]*wrapper
	sql      string
	params   []any
}

type wrapper struct {
	res        resource
	kind       PlanKind
	expr       queryplan.Expr
	text       string
	args       []any
	references int
}

func (j *rewriteJob) checkReader(schema, name string) error {
	for _, rd := range j.rewriter.readers {
		if rd.matches(schema, name) {
			return refuse(CodeUnsupportedSQL, "%s reads a protected table", joinName(schema, name))
		}
	}
	return nil
}

func (j *rewriteJob) checkCTEName(name string) error {
	for _, p := range j.rewriter.protected {
		if strings.EqualFold(p.name, name) {
			return refuse(CodeAmbiguousReference, "the CTE %s shadows the protected table %s", name, p)
		}
	}
	return nil
}

// target resolves a table reference. It returns nil when the table is not protected.
func (j *rewriteJob) target(schema, name string) (*wrapper, error) {
	if err := j.checkReader(schema, name); err != nil {
		return nil, err
	}
	var match *resource
	for i := range j.rewriter.resources {
		res := &j.rewriter.resources[i]
		if !res.name.matches(schema, name) {
			continue
		}
		if match != nil {
			return nil, refuse(CodeAmbiguousReference, "%s matches both %s and %s", joinName(schema, name), match.name, res.name)
		}
		match = res
	}
	if match == nil {
		for _, p := range j.rewriter.protected {
			if p.matches(schema, name) {
				return nil, refuse(CodeMissingPlan, "%s is protected and has no resource mapping", joinName(schema, name))
			}
		}
		return nil, nil
	}

	if w, ok := j.wrappers[match.kind]; ok {
		w.references++
		return w, nil
	}

	plan, ok := j.plans[match.kind]
	if !ok {
		return nil, refuse(CodeMissingPlan, "the statement reads %s and no plan was sent for %q", match.name, match.kind)
	}
	w, err := j.translate(*match, plan)
	if err != nil {
		return nil, err
	}
	w.references = 1
	j.wrappers[match.kind] = w
	return w, nil
}

func (j *rewriteJob) translate(res resource, plan *responsev1.PlanResourcesResponse) (*wrapper, error) {
	filter := plan.GetFilter()
	w := &wrapper{res: res}
	var expr queryplan.Expr
	switch filter.GetKind() {
	case enginev1.PlanResourcesFilter_KIND_ALWAYS_ALLOWED:
		w.kind = KindAlwaysAllowed
		return w, nil
	case enginev1.PlanResourcesFilter_KIND_ALWAYS_DENIED:
		w.kind = KindAlwaysDenied
		expr = queryplan.BoolConst{V: false}
	case enginev1.PlanResourcesFilter_KIND_CONDITIONAL:
		w.kind = KindConditional
		rec := &recordingMapper{inner: res.mapper}
		var err error
		expr, err = queryplan.Build(filter.GetCondition(), rec, queryplan.Options{
			RootTable:          wrapperAlias,
			NullRepresentation: j.rewriter.nullRep,
		})
		if err != nil {
			if len(rec.missing) > 0 {
				return nil, refuse(CodeUnmappedAttribute, "%s: %s is not mapped: %v", res.kind, strings.Join(rec.missing, ", "), err)
			}
			return nil, refuse(CodeUnsupportedPlan, "%s: %v", res.kind, err)
		}
	default:
		if filter == nil {
			w.kind = KindAlwaysDenied
			expr = queryplan.BoolConst{V: false}
			break
		}
		return nil, refuse(CodeUnsupportedPlan, "%s: unrecognised filter kind %s", res.kind, filter.GetKind())
	}

	text, args, err := renderFilter(expr, j.rewriter.dialect.entDialect(), 0)
	if err != nil {
		return nil, refuse(CodeUnsupportedPlan, "%s: %v", res.kind, err)
	}
	w.expr, w.text, w.args = expr, text, args
	return w, nil
}

func (j *rewriteJob) isProtected(schema, name string) bool {
	for _, p := range j.rewriter.protected {
		if p.matches(schema, name) {
			return true
		}
	}
	return false
}

func (j *rewriteJob) mustBeWrapped(schema, name string) bool {
	if !j.isProtected(schema, name) {
		return false
	}
	for _, res := range j.rewriter.resources {
		if res.name.matches(schema, name) {
			if w, ok := j.wrappers[res.kind]; ok && w.kind == KindAlwaysAllowed {
				return false
			}
		}
	}
	return true
}

func joinName(schema, name string) string {
	if schema == "" {
		return name
	}
	return schema + "." + name
}

type recordingMapper struct {
	inner   queryplan.Mapper
	missing []string
}

func (m *recordingMapper) Resolve(reference string) (queryplan.Entry, bool) {
	e, ok := m.inner.Resolve(reference)
	if !ok && strings.HasPrefix(reference, "request.") && !slices.Contains(m.missing, reference) {
		m.missing = append(m.missing, reference)
	}
	return e, ok
}

// The dialects print the caller's statement from its syntax tree with a marker identifier where
// each filter goes, then splice in the filter text exactly as renderFilter wrote it, so no printer
// can re-associate the operators of a filter.
const filterMarkerPrefix = "cerbos_filter_"

func filterMarker(i int) string { return fmt.Sprintf("%s%d_", filterMarkerPrefix, i) }

func wrapperSkeleton(marker string) string {
	return "SELECT * FROM " + wrapperPlaceholder + " AS " + wrapperAlias + " WHERE " + marker
}

// spliceFilters replaces each marker, bare or quoted, with its filter. Every marker must occur
// exactly once.
func spliceFilters(text string, filters map[string]string) (string, error) {
	for marker, filter := range filters {
		found := 0
		for _, spelling := range []string{`"` + marker + `"`, "`" + marker + "`", marker} {
			if n := strings.Count(text, spelling); n > 0 {
				found += n
				text = strings.Replace(text, spelling, filter, 1)
			}
		}
		if found != 1 {
			return "", refuse(CodeUnsupportedSQL, "verify: the filter marker %s was printed %d times", marker, found)
		}
	}
	return text, nil
}

func (c *Config) Key() string { return "rewrite" }
