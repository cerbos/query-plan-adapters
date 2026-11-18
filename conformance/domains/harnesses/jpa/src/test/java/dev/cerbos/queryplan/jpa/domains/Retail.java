package dev.cerbos.queryplan.jpa.domains;

import static dev.cerbos.queryplan.jpa.AttributeMapping.field;
import static dev.cerbos.queryplan.jpa.AttributeMapping.relation;
import static dev.cerbos.queryplan.jpa.domains.DomainMapping.omitted;

import dev.cerbos.queryplan.jpa.JpaQueryPlanAdapter.Options;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

class Retail implements DomainMapping {

    @Entity @Table(name = "stores")
    public static class Store {
        @Id String id;
        String region;
        String country;
    }

    @Entity @Table(name = "customers")
    public static class Customer {
        @Id String id;
        String email;
        String tier;
        int loyaltyPoints;
    }

    @Entity(name = "CustomerOrder") @Table(name = "order")
    public static class Order {
        @Id String id;
        @ManyToOne Customer customer;
        @ManyToOne Store store;
        String status;
        String channel;
        long totalCents;
        String currency;
        String couponCode;
        Instant placedAt;
        boolean gift;
        @OneToMany(mappedBy = "order") List<OrderLine> lines;
    }

    @Entity @Table(name = "order_line")
    public static class OrderLine {
        @Id String id;
        @ManyToOne Order order;
        int lineNo;
        String sku;
        String category;
        int quantity;
        long unitPriceCents;
    }

    public String domain() { return "retail"; }

    public List<Class<?>> entities() {
        return List.of(Store.class, Customer.class, Order.class, OrderLine.class);
    }

    public Class<?> root() { return Order.class; }

    public Options options() {
        String a = "request.resource.attr.";
        return Options.of(Map.ofEntries(
                Map.entry(a + "status", field("status")),
                Map.entry(a + "channel", field("channel")),
                Map.entry(a + "totalCents", field("totalCents")),
                Map.entry(a + "currency", field("currency")),
                Map.entry(a + "couponCode", omitted("couponCode")),
                Map.entry(a + "placedAt", field("placedAt")),
                Map.entry(a + "isGift", field("gift")),
                Map.entry(a + "customer.id", field("customer.id")),
                Map.entry(a + "customer.tier", field("customer.tier")),
                Map.entry(a + "customer.loyaltyPoints", field("customer.loyaltyPoints")),
                Map.entry(a + "store.region", field("store.region")),
                Map.entry(a + "store.country", field("store.country")),
                Map.entry(a + "lines", relation("lines", Map.of()))));
    }
}
