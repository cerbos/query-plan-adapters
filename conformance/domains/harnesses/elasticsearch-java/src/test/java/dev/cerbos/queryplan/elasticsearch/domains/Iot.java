package dev.cerbos.queryplan.elasticsearch.domains;

import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

import java.util.Set;

class Iot implements EsDomain {

    public String name() { return "iot"; }

    public String properties() {
        return """
            {"id": {"type":"keyword"}, "serial": {"type":"keyword"}, "model": {"type":"keyword"}, "firmware": {"type":"keyword"}, "lastSeenAt": {"type":"date"}, "online": {"type":"boolean"},
             "batteryPct": {"type":"integer"}, "decommissioned": {"type":"boolean"},
             "labels": {"properties": {"env": {"type":"keyword"}, "tier": {"type":"keyword"}, "restricted": {"type":"keyword"}}},
             "site": {"properties": {"region": {"type":"keyword"}, "org": {"properties": {"id": {"type":"keyword"}, "plan": {"type":"keyword"}}}}},
             "alerts": {"type": "nested", "properties": {"severity": {"type":"integer"}, "acknowledged": {"type":"boolean"}, "code": {"type":"keyword"}}}}
            """;
    }

    public Options options() {
        return EsDomain.fromMapping(properties());
    }
}
