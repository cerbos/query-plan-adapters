package dev.cerbos.queryplan.elasticsearch.domains;

import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

import java.util.Set;

class Fintech implements EsDomain {

    public String name() { return "fintech"; }

    public String properties() {
        return """
            {"id": {"type":"keyword"}, "amount": {"type":"double"}, "currency": {"type":"keyword"}, "status": {"type":"keyword"}, "riskScore": {"type":"double"}, "createdAt": {"type":"date"},
             "initiatedBy": {"type":"keyword"}, "flags": {"type":"keyword"},
             "counterparty": {"properties": {"name": {"type":"keyword"}, "iban": {"type":"keyword"}, "country": {"type":"keyword"}}},
             "approvals": {"type": "nested", "properties": {"approverId": {"type":"keyword"}, "decision": {"type":"keyword"}}},
             "account": {"properties": {"currency": {"type":"keyword"}, "status": {"type":"keyword"}, "overdraftLimit": {"type":"double"},
               "customer": {"properties": {"id": {"type":"keyword"}, "kycLevel": {"type":"integer"}, "pep": {"type":"boolean"},
                 "branch": {"properties": {"code": {"type":"keyword"}, "country": {"type":"keyword"}}}}}}}}
            """;
    }

    public Options options() {
        return EsDomain.fromMapping(properties())
                .withCollectionFields(Set.of("flags"));
    }
}
