package dev.cerbos.queryplan.elasticsearch.domains;

import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

import java.util.Set;

class Government implements EsDomain {

    public String name() { return "government"; }

    public String properties() {
        return """
            {"id": {"type":"keyword"}, "caseNumber": {"type":"keyword"}, "status": {"type":"keyword"}, "clearanceLevel": {"type":"integer"}, "program": {"type":"keyword"}, "openedOn": {"type":"date"},
             "closedOn": {"type":"date"}, "pii": {"type":"boolean"}, "assignedTo": {"type":"keyword"}, "monthlyAmount": {"type":"double"}, "flags": {"type":"keyword"},
             "agency": {"properties": {"code": {"type":"keyword"}, "state": {"type":"keyword"}}},
             "applicant": {"properties": {"countyFips": {"type":"keyword"}, "householdSize": {"type":"integer"}, "citizenship": {"type":"keyword"}}}}
            """;
    }

    public Options options() {
        return EsDomain.fromMapping(properties())
                .withCollectionFields(Set.of("flags"))
                .withExplicitNullAttributes(Set.of("request.resource.attr.closedOn",
                        "request.resource.attr.assignedTo", "request.resource.attr.monthlyAmount",
                        "request.resource.attr.applicant.citizenship"));
    }
}
