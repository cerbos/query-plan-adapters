package dev.cerbos.queryplan.elasticsearch.domains;

import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

import java.util.Set;

class Healthcare implements EsDomain {

    public String name() { return "healthcare"; }

    public String properties() {
        return """
            {"id": {"type":"keyword"}, "noteType": {"type":"keyword"}, "authorId": {"type":"keyword"}, "sensitivity": {"type":"integer"}, "signedAt": {"type":"date"},
             "deleted": {"type":"boolean"}, "careTeam": {"type":"keyword"},
             "codes": {"type": "nested", "properties": {"system": {"type":"keyword"}, "code": {"type":"keyword"}}},
             "patient": {"properties": {"mrn": {"type":"keyword"}, "consentResearch": {"type":"boolean"}, "vip": {"type":"boolean"}, "birthYear": {"type":"integer"},
               "ward": {"properties": {"id": {"type":"keyword"}, "secure": {"type":"boolean"},
                 "hospital": {"properties": {"region": {"type":"keyword"}}}}}}}}
            """;
    }

    public Options options() {
        return EsDomain.fromMapping(properties())
                .withCollectionFields(Set.of("careTeam"));
    }
}
