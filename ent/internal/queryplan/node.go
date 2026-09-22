// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

// Package queryplan is the Go adapters' shared plan translator: it walks a Cerbos `PlanResources`
// condition tree and lowers it into the small, dialect-free SQL expression tree in expr.go. Each
// module's own render.go then emits that tree for its engine — an ent predicate in the ent module,
// a PostgreSQL fragment in the pgx module.
//
// The package is vendored byte-for-byte into both modules (see expr.go) so that each ships as a
// standalone Go module with no dependency on anything else in this repository.
//
// The files split along the translation's stages: node.go decodes the protobuf plan, translate.go
// walks it (operator dispatch, macros, relations, NULL-operand rejection), values.go folds and
// lowers operands (comparisons, arithmetic, LIKE, timestamps, hierarchies), scalar_types.go
// handles caller-declared column types, and mapper.go resolves plan variables onto storage.
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

	// A oneof wrapper can be a typed nil — the type switch still matches it, so each arm has to
	// check before dereferencing. Protobuf's own decoder does not produce these, but a plan can
	// be built by hand, and a panic in a library on the authorization path takes the caller's
	// process down.
	switch t := op.GetNode().(type) {
	case *enginev1.PlanResourcesFilter_Expression_Operand_Expression:
		if t == nil {
			return nil, fmt.Errorf("nil expression operand in query plan")
		}
		return decodeExpression(t.Expression)

	case *enginev1.PlanResourcesFilter_Expression_Operand_Variable:
		if t == nil {
			return nil, fmt.Errorf("nil variable operand in query plan")
		}
		return &node{kind: nodeVariable, variable: t.Variable}, nil

	case *enginev1.PlanResourcesFilter_Expression_Operand_Value:
		if t == nil {
			return nil, fmt.Errorf("nil value operand in query plan")
		}
		if t.Value == nil {
			// An unset Value would decode to Go nil and silently become an IS NULL test.
			return nil, fmt.Errorf("value operand carries no value in query plan")
		}
		return valueNode(decodeValue(t.Value)), nil

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

// valueNode returns a value node carrying v.
func valueNode(v any) *node {
	return &node{kind: nodeValue, value: v}
}
