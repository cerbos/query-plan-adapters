package domains

import (
	"context"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/media"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/media/article"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/media/section"
)

var Media = Domain{
	Name: "media", Table: article.Table, Nulls: cerbosent.NullOmitted,
	Mapper: func() (cerbosent.Mapper, error) {
		return media.CerbosSchema.Mapper("Article", cerbosent.SchemaMap{
			"request.resource.attr.section.slug": {Edge: article.EdgeSection, Column: section.FieldID, NullConvention: omitted},
		}, cerbosent.WithInferredNullConvention(omitted))
	},
	IDs: func(ctx context.Context, drv dialect.Driver, p *entsql.Predicate) ([]string, error) {
		return media.NewClient(media.Driver(drv)).Article.Query().Where(where(p)).IDs(ctx)
	},
}
