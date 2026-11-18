package dev.cerbos.queryplan.elasticsearch.domains;

import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

import java.util.Set;

class Hr implements EsDomain {

    public String name() { return "hr"; }

    public String properties() {
        return """
            {"id": {"type":"keyword"}, "level": {"type":"integer"}, "employmentType": {"type":"keyword"}, "salaryBand": {"type":"keyword"}, "hiredOn": {"type":"date"}, "terminatedOn": {"type":"date"},
             "email": {"type":"keyword"}, "skills": {"type":"keyword"},
             "department": {"properties": {"id": {"type":"keyword"}, "costCenter": {"type":"keyword"}}},
             "location": {"properties": {"country": {"type":"keyword"}, "city": {"type":"keyword"}}},
             "manager": {"properties": {"id": {"type":"keyword"}, "manager": {"properties": {"id": {"type":"keyword"}}}}}}
            """;
    }

    public Options options() {
        return EsDomain.fromMapping(properties())
                .withCollectionFields(Set.of("skills"));
    }
}
