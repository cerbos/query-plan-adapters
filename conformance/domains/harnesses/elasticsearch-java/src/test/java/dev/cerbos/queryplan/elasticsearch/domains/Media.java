package dev.cerbos.queryplan.elasticsearch.domains;

import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

import java.util.Set;

class Media implements EsDomain {

    public String name() { return "media"; }

    public String properties() {
        return """
            {"id": {"type":"keyword"}, "slug": {"type":"keyword"}, "status": {"type":"keyword"}, "language": {"type":"keyword"}, "embargoUntil": {"type":"date"}, "publishedAt": {"type":"date"},
             "paywalled": {"type":"boolean"}, "wordCount": {"type":"integer"}, "keywords": {"type":"keyword"},
             "section": {"properties": {"slug": {"type":"keyword"}, "desk": {"type":"keyword"}}},
             "author": {"properties": {"id": {"type":"keyword"}, "staff": {"type":"boolean"}}},
             "contributors": {"type": "nested", "properties": {"id": {"type":"keyword"}, "staff": {"type":"boolean"}}}}
            """;
    }

    public Options options() {
        return EsDomain.fromMapping(properties())
                .withCollectionFields(Set.of("keywords"));
    }
}
