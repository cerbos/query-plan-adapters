package dev.cerbos.queryplan.elasticsearch.domains;

import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

import java.util.Set;

class Legal implements EsDomain {

    public String name() { return "legal"; }

    public String properties() {
        return """
            {"id": {"type":"keyword"}, "title": {"type":"keyword"}, "privilege": {"type":"keyword"}, "classification": {"type":"integer"}, "legalHold": {"type":"boolean"}, "createdBy": {"type":"keyword"},
             "retentionUntil": {"type":"date"},
             "shares": {"type": "nested", "properties": {"partyType": {"type":"keyword"}, "partyId": {"type":"keyword"}}},
             "matter": {"properties": {"status": {"type":"keyword"}, "practiceArea": {"type":"keyword"}, "leadPartnerId": {"type":"keyword"},
               "team": {"type":"keyword"}, "screened": {"type":"keyword"},
               "client": {"properties": {"id": {"type":"keyword"}, "industry": {"type":"keyword"}}}}}}
            """;
    }

    public Options options() {
        return EsDomain.fromMapping(properties())
                .withCollectionFields(Set.of("matter.team", "matter.screened"))
                .withExplicitNullAttributes(Set.of("request.resource.attr.privilege",
                        "request.resource.attr.retentionUntil", "request.resource.attr.matter.leadPartnerId",
                        "request.resource.attr.matter.client.industry"));
    }
}
