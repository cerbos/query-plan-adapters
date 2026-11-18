package domains

import (
	"context"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
	"github.com/cerbos/query-plan-adapters/conformance/domains/harnesses/ent/gen/healthcare"
	"github.com/cerbos/query-plan-adapters/conformance/domains/harnesses/ent/gen/healthcare/clinicalnote"
)

var Healthcare = Domain{
	Name: "healthcare", Table: clinicalnote.Table, Nulls: cerbosent.NullOmitted,
	Mapper: func() (cerbosent.Mapper, error) {
		return healthcare.CerbosSchema.Mapper("ClinicalNote", cerbosent.SchemaMap{
			"request.resource.attr.noteType": {Column: clinicalnote.FieldType},
			"request.resource.attr.deleted":  {Column: clinicalnote.FieldIsDeleted},
			"request.resource.attr.careTeam": scalarList("note_care_team", "note_id", "clinician_id", clinicalnote.FieldID),
		}, cerbosent.WithInferredNullConvention(omitted))
	},
	IDs: func(ctx context.Context, drv dialect.Driver, p *entsql.Predicate) ([]string, error) {
		return healthcare.NewClient(healthcare.Driver(drv)).ClinicalNote.Query().Where(where(p)).IDs(ctx)
	},
}
