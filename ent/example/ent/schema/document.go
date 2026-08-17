// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

// Package schema holds the demo domain's one entity, as a consumer would write it.
package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
)

// Document is the demo domain's only resource kind: flat scalar fields, no edges.
//
// The field names are deliberately NOT the Cerbos attribute names. `request.resource.attr.ownerId`
// is `owner_id` here and `public` is `is_public`, which is ordinary Go and ent naming — and is what
// makes the attribute map in main.go load-bearing rather than decorative.
//
// `region` and `archived` are never named by demo/policies/document.yaml. They are the
// application's own columns, and composing them with the adapter's predicate is usage shape 5.
type Document struct {
	ent.Schema
}

// Fields of the Document.
func (Document) Fields() []ent.Field {
	return []ent.Field{
		// A string id rather than ent's default int, because demo/seeds.json names its rows
		// "d1".."d8" and demo/cases.json asserts those ids.
		field.String("id").Immutable(),
		field.String("owner_id").Immutable(),
		field.Bool("is_public"),
		field.String("region"),
		field.Bool("archived"),
	}
}
