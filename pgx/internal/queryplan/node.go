// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

// Package queryplan is this adapter's plan translator: it walks a Cerbos `PlanResources`
// condition tree and lowers it into the small SQL expression tree in expr.go, which render.go
// then emits as PostgreSQL.
//
// It is internal to the cerbospgx module and deliberately self-contained — the adapter ships as a
// standalone Go module with no dependency on anything else in this repository, so a consumer only
// ever pulls in github.com/cerbos/query-plan-adapters/pgx.
//
// The semantics encoded here (value-first operand inversion, LIKE metacharacter escaping,
// three-valued logic under negation) are proved against ../../../conformance/, the shared
// adversarial corpus every adapter in this repository is measured by.
package queryplan

import (
	"fmt"

	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// node is a neutral view of a plan operand. The protobuf oneof is awkward to traverse
// repeatedly, and the reference adapters all work against a decoded tree (sqlalchemy uses
// MessageToDict), so decode once up front and keep the traversal readable.
type node struct {
	value    any
	operator string
	variable string
	operands []*node
	kind     nodeKind
}

type nodeKind uint8

const (
	nodeExpression nodeKind = iota
	nodeVariable
	nodeValue
)

func (n *node) isValue() bool    { return n.kind == nodeValue }
func (n *node) isVariable() bool { return n.kind == nodeVariable }
func (n *node) isExpr() bool     { return n.kind == nodeExpression }

// decodeOperand converts a protobuf operand into a node tree.
func decodeOperand(op *enginev1.PlanResourcesFilter_Expression_Operand) (*node, error) {
	if op == nil {
		return nil, fmt.Errorf("nil operand in query plan")
	}

	switch t := op.GetNode().(type) {
	case *enginev1.PlanResourcesFilter_Expression_Operand_Expression:
		return decodeExpression(t.Expression)

	case *enginev1.PlanResourcesFilter_Expression_Operand_Variable:
		return &node{kind: nodeVariable, variable: t.Variable}, nil

	case *enginev1.PlanResourcesFilter_Expression_Operand_Value:
		return &node{kind: nodeValue, value: decodeValue(t.Value)}, nil

	default:
		return nil, fmt.Errorf("unrecognised operand shape %T in query plan", t)
	}
}

func decodeExpression(e *enginev1.PlanResourcesFilter_Expression) (*node, error) {
	if e == nil {
		return nil, fmt.Errorf("nil expression in query plan")
	}

	n := &node{kind: nodeExpression, operator: e.GetOperator()}
	for _, child := range e.GetOperands() {
		decoded, err := decodeOperand(child)
		if err != nil {
			return nil, err
		}
		n.operands = append(n.operands, decoded)
	}

	return n, nil
}

// decodeValue lowers a structpb value into a plain Go value. Numbers stay float64 — Cerbos
// transports every CEL number as a double, and collapsing whole doubles to int here would erase
// the distinction the arithmetic probes depend on (see conformance's `arith-*` actions and the
// `_float_div` note in the SQLAlchemy adapter).
func decodeValue(v *structpb.Value) any {
	if v == nil {
		return nil
	}

	switch k := v.GetKind().(type) {
	case *structpb.Value_NullValue:
		return nil
	case *structpb.Value_BoolValue:
		return k.BoolValue
	case *structpb.Value_NumberValue:
		return k.NumberValue
	case *structpb.Value_StringValue:
		return k.StringValue
	case *structpb.Value_ListValue:
		out := make([]any, 0, len(k.ListValue.GetValues()))
		for _, item := range k.ListValue.GetValues() {
			out = append(out, decodeValue(item))
		}
		return out
	case *structpb.Value_StructValue:
		out := make(map[string]any, len(k.StructValue.GetFields()))
		for name, field := range k.StructValue.GetFields() {
			out[name] = decodeValue(field)
		}
		return out
	default:
		return nil
	}
}

// cloneWithValue returns a value node carrying v, used when substituting a lambda variable.
func cloneWithValue(v any) *node {
	return &node{kind: nodeValue, value: v}
}
