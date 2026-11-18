package dev.cerbos.queryplan.elasticsearch.domains;

import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

import java.util.Set;

class Retail implements EsDomain {

    public String name() { return "retail"; }

    public String properties() {
        return """
            {"id": {"type":"keyword"}, "status": {"type":"keyword"}, "channel": {"type":"keyword"}, "totalCents": {"type":"long"}, "currency": {"type":"keyword"}, "couponCode": {"type":"keyword"},
             "placedAt": {"type":"date"}, "isGift": {"type":"boolean"},
             "customer": {"properties": {"id": {"type":"keyword"}, "tier": {"type":"keyword"}, "loyaltyPoints": {"type":"integer"}}},
             "store": {"properties": {"region": {"type":"keyword"}, "country": {"type":"keyword"}}},
             "lines": {"type": "nested", "properties": {"sku": {"type":"keyword"}, "category": {"type":"keyword"}, "quantity": {"type":"integer"},
               "unitPriceCents": {"type":"long"}}}}
            """;
    }

    public Options options() {
        return EsDomain.fromMapping(properties());
    }
}
