package dev.cerbos.queryplan.jpa.domains;

import static dev.cerbos.queryplan.jpa.AttributeMapping.field;
import static dev.cerbos.queryplan.jpa.AttributeMapping.relation;
import static dev.cerbos.queryplan.jpa.domains.DomainMapping.omitted;

import dev.cerbos.queryplan.jpa.AttributeMapping;
import dev.cerbos.queryplan.jpa.JpaQueryPlanAdapter.Options;

import jakarta.persistence.*;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Iot implements DomainMapping {

    @Entity @Table(name = "orgs")
    public static class Org {
        @Id String id;
        String name;
        String plan;
    }

    @Entity @Table(name = "sites")
    public static class Site {
        @Id String id;
        @ManyToOne Org org;
        String region;
        String name;
    }

    // The JSON object column as a Hibernate JSON aggregate; only the keys the policies read.
    @Embeddable
    public static class Labels {
        String env;
        String tier;
        String restricted;
    }

    @Entity @Table(name = "devices")
    public static class Device {
        @Id String id;
        String serial;
        String model;
        String firmware;
        @ManyToOne Site site;
        Instant lastSeenAt;
        boolean online;
        Integer batteryPct;
        @Embedded @JdbcTypeCode(SqlTypes.JSON) Labels labels;
        boolean decommissioned;
        @OneToMany(mappedBy = "device") List<DeviceAlert> alerts;
    }

    @Entity @Table(name = "device_alerts")
    public static class DeviceAlert {
        @Id String id;
        @ManyToOne Device device;
        int severity;
        boolean acknowledged;
        String code;
    }

    // Hibernate 6.6 has JSON aggregates only on PostgreSQL here; on H2 and MySQL the column stays opaque.
    @Entity(name = "Device") @Table(name = "devices")
    public static class PlainDevice {
        @Id String id;
        String serial;
        String model;
        String firmware;
        @ManyToOne Site site;
        Instant lastSeenAt;
        boolean online;
        Integer batteryPct;
        String labels;
        boolean decommissioned;
        @OneToMany(mappedBy = "device") List<PlainDeviceAlert> alerts;
    }

    @Entity(name = "DeviceAlert") @Table(name = "device_alerts")
    public static class PlainDeviceAlert {
        @Id String id;
        @ManyToOne PlainDevice device;
        int severity;
        boolean acknowledged;
        String code;
    }

    final boolean jsonAggregates = "postgres".equals(System.getenv("ADAPTER_DOMAIN_STORE"));

    public String domain() { return "iot"; }

    public List<Class<?>> entities() {
        return jsonAggregates
                ? List.of(Org.class, Site.class, Labels.class, Device.class, DeviceAlert.class)
                : List.of(Org.class, Site.class, PlainDevice.class, PlainDeviceAlert.class);
    }

    public Class<?> root() { return jsonAggregates ? Device.class : PlainDevice.class; }

    public Options options() {
        String a = "request.resource.attr.";
        Map<String, AttributeMapping> labels = jsonAggregates ? Map.of(
                a + "labels.env", omitted("labels.env"),
                a + "labels.tier", omitted("labels.tier"),
                a + "labels.restricted", omitted("labels.restricted")) : Map.of();
        Map<String, AttributeMapping> m = new HashMap<>(labels);
        m.putAll(Map.ofEntries(
                Map.entry(a + "serial", field("serial")),
                Map.entry(a + "model", field("model")),
                Map.entry(a + "firmware", field("firmware")),
                Map.entry(a + "lastSeenAt", omitted("lastSeenAt")),
                Map.entry(a + "online", field("online")),
                Map.entry(a + "batteryPct", omitted("batteryPct")),
                Map.entry(a + "decommissioned", field("decommissioned")),
                Map.entry(a + "site.region", field("site.region")),
                Map.entry(a + "site.org.id", field("site.org.id")),
                Map.entry(a + "site.org.plan", field("site.org.plan")),
                Map.entry(a + "alerts", relation("alerts", Map.of()))));
        return Options.of(m);
    }
}
