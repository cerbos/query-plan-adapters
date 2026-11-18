package dev.cerbos.queryplan.elasticsearch.domains;

import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

import java.util.Set;

class Logistics implements EsDomain {

    public String name() { return "logistics"; }

    public String properties() {
        return """
            {"id": {"type":"keyword"}, "trackingNumber": {"type":"keyword"}, "status": {"type":"keyword"}, "weightKg": {"type":"double"}, "hazmat": {"type":"boolean"}, "customsHold": {"type":"boolean"},
             "shipper": {"type":"keyword"}, "eta": {"type":"date"}, "declaredValue": {"type":"double"}, "handling": {"type":"keyword"},
             "origin": {"properties": {"code": {"type":"keyword"}, "country": {"type":"keyword"}, "region": {"type":"keyword"}}},
             "destination": {"properties": {"code": {"type":"keyword"}, "country": {"type":"keyword"}, "region": {"type":"keyword"}}},
             "carrier": {"properties": {"scac": {"type":"keyword"}}},
             "checkpoints": {"type": "nested", "properties": {"event": {"type":"keyword"}, "scannedAt": {"type":"date"},
               "hub": {"properties": {"code": {"type":"keyword"}}}}}}
            """;
    }

    public Options options() {
        return EsDomain.fromMapping(properties())
                .withCollectionFields(Set.of("handling"));
    }
}
