package dev.cerbos.queryplan.jpa.domains;

import static dev.cerbos.queryplan.jpa.AttributeMapping.field;
import static dev.cerbos.queryplan.jpa.AttributeMapping.relation;
import static dev.cerbos.queryplan.jpa.domains.DomainMapping.omitted;

import dev.cerbos.queryplan.jpa.JpaQueryPlanAdapter.Options;

import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

class Logistics implements DomainMapping {

    @Entity @Table(name = "hubs")
    public static class Hub {
        @Id String id;
        String code;
        String country;
        String region;
    }

    @Entity @Table(name = "carriers")
    public static class Carrier {
        @Id String id;
        String name;
        String scac;
    }

    @Entity @Table(name = "shipments")
    public static class Shipment {
        @Id String id;
        String trackingNo;
        String status;
        @ManyToOne @JoinColumn(name = "origin_hub_id") Hub origin;
        @ManyToOne @JoinColumn(name = "destination_hub_id") Hub destination;
        @ManyToOne Carrier carrier;
        double weightKg;
        boolean hazmat;
        Boolean customsHold;
        String shipperAccount;
        Instant eta;
        BigDecimal declaredValue;
        @ElementCollection
        @CollectionTable(name = "shipment_handling", joinColumns = @JoinColumn(name = "shipment_id"))
        @Column(name = "code")
        Set<String> handling;
        @OneToMany(mappedBy = "shipment") List<Checkpoint> checkpoints;
    }

    @Entity @Table(name = "checkpoints")
    public static class Checkpoint {
        @Id String id;
        @ManyToOne Shipment shipment;
        @ManyToOne Hub hub;
        String event;
        Instant scannedAt;
    }

    public String domain() { return "logistics"; }

    public List<Class<?>> entities() {
        return List.of(Hub.class, Carrier.class, Shipment.class, Checkpoint.class);
    }

    public Class<?> root() { return Shipment.class; }

    public Options options() {
        String a = "request.resource.attr.";
        return Options.of(Map.ofEntries(
                Map.entry(a + "trackingNumber", field("trackingNo")),
                Map.entry(a + "status", field("status")),
                Map.entry(a + "weightKg", field("weightKg")),
                Map.entry(a + "hazmat", field("hazmat")),
                Map.entry(a + "customsHold", omitted("customsHold")),
                Map.entry(a + "shipper", field("shipperAccount")),
                Map.entry(a + "eta", omitted("eta")),
                Map.entry(a + "declaredValue", field("declaredValue")),
                Map.entry(a + "origin.code", field("origin.code")),
                Map.entry(a + "origin.country", field("origin.country")),
                Map.entry(a + "origin.region", field("origin.region")),
                Map.entry(a + "destination.code", field("destination.code")),
                Map.entry(a + "destination.country", field("destination.country")),
                Map.entry(a + "destination.region", field("destination.region")),
                Map.entry(a + "carrier.scac", field("carrier.scac")),
                Map.entry(a + "handling", relation("handling")),
                Map.entry(a + "checkpoints", relation("checkpoints", Map.of()))));
    }
}
