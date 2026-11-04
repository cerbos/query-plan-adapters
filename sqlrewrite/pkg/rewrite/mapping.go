// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package rewrite

import (
	"fmt"

	"github.com/cerbos/query-plan-adapters/sqlrewrite/pkg/rewrite/internal/queryplan"
)

type ValueType string

const (
	ValueDefault   ValueType = ""
	ValueTimestamp ValueType = "timestamp"
	ValueBool      ValueType = "bool"
	ValueString    ValueType = "string"
	ValueNumber    ValueType = "number"
)

type NullConvention string

const (
	NullConventionUnset    NullConvention = ""
	NullConventionExplicit NullConvention = "explicit"
	NullConventionOmitted  NullConvention = "omitted"
)

type NullRepresentation string

const (
	NullOmitted  NullRepresentation = "omitted"
	NullExplicit NullRepresentation = "explicit"
)

type RestrictOp string

const (
	RestrictEq        RestrictOp = "eq"
	RestrictNe        RestrictOp = "ne"
	RestrictIsNull    RestrictOp = "isNull"
	RestrictIsNotNull RestrictOp = "isNotNull"
	RestrictIn        RestrictOp = "in"
	RestrictNotIn     RestrictOp = "notIn"
)

type Entry struct {
	Relation       *Relation      `yaml:"relation" json:"relation,omitempty"`
	ScalarRelation *Relation      `yaml:"scalarRelation" json:"scalarRelation,omitempty"`
	Column         string         `yaml:"column" json:"column,omitempty"`
	ValueType      ValueType      `yaml:"valueType" json:"valueType,omitempty"`
	NullConvention NullConvention `yaml:"nullConvention" json:"nullConvention,omitempty"`
}

type Relation struct {
	Field          *Entry           `yaml:"field" json:"field,omitempty"`
	Fields         map[string]Entry `yaml:"fields" json:"fields,omitempty"`
	Table          string           `yaml:"table" json:"table"`
	SourceColumn   string           `yaml:"sourceColumn" json:"sourceColumn"`
	TargetColumn   string           `yaml:"targetColumn" json:"targetColumn"`
	Position       string           `yaml:"position" json:"position,omitempty"`
	Via            []Hop            `yaml:"via" json:"via,omitempty"`
	SubqueryFilter []Restriction    `yaml:"subqueryFilter" json:"subqueryFilter,omitempty"`
}

type Hop struct {
	Table          string        `yaml:"table" json:"table"`
	ChildColumn    string        `yaml:"childColumn" json:"childColumn"`
	JoinColumn     string        `yaml:"joinColumn" json:"joinColumn"`
	SubqueryFilter []Restriction `yaml:"subqueryFilter" json:"subqueryFilter,omitempty"`
}

type Restriction struct {
	Value  any        `yaml:"value" json:"value,omitempty"`
	Column string     `yaml:"column" json:"column"`
	Op     RestrictOp `yaml:"op" json:"op,omitempty"`
	Values []any      `yaml:"values" json:"values,omitempty"`
}

func compileMapper(attrs map[string]Entry) (queryplan.MapperMap, error) {
	out := make(queryplan.MapperMap, len(attrs))
	for ref, e := range attrs {
		qe, err := compileEntry(e)
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", ref, err)
		}
		out[ref] = qe
	}
	return out, nil
}

func compileEntry(e Entry) (queryplan.Entry, error) {
	vt, err := compileValueType(e.ValueType)
	if err != nil {
		return queryplan.Entry{}, err
	}
	nc, err := compileNullConvention(e.NullConvention)
	if err != nil {
		return queryplan.Entry{}, err
	}
	out := queryplan.Entry{Column: e.Column, ValueType: vt, NullConvention: nc}
	if e.Relation != nil {
		if out.Relation, err = compileRelation(*e.Relation); err != nil {
			return queryplan.Entry{}, fmt.Errorf("relation: %w", err)
		}
	}
	if e.ScalarRelation != nil {
		if out.ScalarRelation, err = compileRelation(*e.ScalarRelation); err != nil {
			return queryplan.Entry{}, fmt.Errorf("scalarRelation: %w", err)
		}
	}
	if out.Column == "" && out.Relation == nil && out.ScalarRelation == nil {
		return queryplan.Entry{}, fmt.Errorf("an entry needs a column, a relation or a scalarRelation")
	}
	return out, nil
}

func compileRelation(r Relation) (*queryplan.Relation, error) {
	if r.Table == "" || r.SourceColumn == "" || r.TargetColumn == "" {
		return nil, fmt.Errorf("table, sourceColumn and targetColumn are required")
	}
	out := &queryplan.Relation{
		Table:        r.Table,
		SourceColumn: r.SourceColumn,
		TargetColumn: r.TargetColumn,
		Position:     r.Position,
	}
	if r.Field != nil {
		f, err := compileEntry(*r.Field)
		if err != nil {
			return nil, fmt.Errorf("field: %w", err)
		}
		out.Field = &f
	}
	if len(r.Fields) > 0 {
		out.Fields = make(map[string]queryplan.Entry, len(r.Fields))
		for name, f := range r.Fields {
			qf, err := compileEntry(f)
			if err != nil {
				return nil, fmt.Errorf("fields.%s: %w", name, err)
			}
			out.Fields[name] = qf
		}
	}
	var err error
	if out.SubqueryFilter, err = compileRestrictions(r.SubqueryFilter); err != nil {
		return nil, err
	}
	for i, h := range r.Via {
		if h.Table == "" || h.ChildColumn == "" || h.JoinColumn == "" {
			return nil, fmt.Errorf("via[%d]: table, childColumn and joinColumn are required", i)
		}
		rs, err := compileRestrictions(h.SubqueryFilter)
		if err != nil {
			return nil, fmt.Errorf("via[%d]: %w", i, err)
		}
		out.Via = append(out.Via, queryplan.Hop{Table: h.Table, ChildColumn: h.ChildColumn, JoinColumn: h.JoinColumn, SubqueryFilter: rs})
	}
	return out, nil
}

func compileRestrictions(rs []Restriction) ([]queryplan.Restriction, error) {
	out := make([]queryplan.Restriction, 0, len(rs))
	for i, r := range rs {
		var op queryplan.RestrictOp
		switch r.Op {
		case RestrictEq, "":
			op = queryplan.RestrictEq
		case RestrictNe:
			op = queryplan.RestrictNe
		case RestrictIsNull:
			op = queryplan.RestrictIsNull
		case RestrictIsNotNull:
			op = queryplan.RestrictIsNotNull
		case RestrictIn:
			op = queryplan.RestrictIn
		case RestrictNotIn:
			op = queryplan.RestrictNotIn
		default:
			return nil, fmt.Errorf("subqueryFilter[%d]: unknown op %q", i, r.Op)
		}
		if r.Column == "" {
			return nil, fmt.Errorf("subqueryFilter[%d]: column is required", i)
		}
		out = append(out, queryplan.Restriction{Column: r.Column, Op: op, Value: r.Value, Values: r.Values})
	}
	return out, nil
}

func compileValueType(v ValueType) (queryplan.ValueType, error) {
	switch v {
	case ValueDefault:
		return queryplan.ValueDefault, nil
	case ValueTimestamp:
		return queryplan.ValueTimestamp, nil
	case ValueBool:
		return queryplan.ValueBool, nil
	case ValueString:
		return queryplan.ValueString, nil
	case ValueNumber:
		return queryplan.ValueNumber, nil
	default:
		return 0, fmt.Errorf("unknown valueType %q", v)
	}
}

func compileNullConvention(n NullConvention) (queryplan.NullConvention, error) {
	switch n {
	case NullConventionUnset:
		return queryplan.NullConventionUnset, nil
	case NullConventionExplicit:
		return queryplan.NullConventionExplicit, nil
	case NullConventionOmitted:
		return queryplan.NullConventionOmitted, nil
	default:
		return 0, fmt.Errorf("unknown nullConvention %q", n)
	}
}

func compileNullRepresentation(n NullRepresentation) (queryplan.NullRepresentation, error) {
	switch n {
	case NullOmitted, "":
		return queryplan.NullOmitted, nil
	case NullExplicit:
		return queryplan.NullExplicit, nil
	default:
		return 0, fmt.Errorf("unknown nullRepresentation %q", n)
	}
}
