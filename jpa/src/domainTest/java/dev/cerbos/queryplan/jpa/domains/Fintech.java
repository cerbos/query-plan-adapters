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

class Fintech implements DomainMapping {

    @Entity @Table(name = "branches")
    public static class Branch {
        @Id String code;
        String country;
    }

    @Entity @Table(name = "customers")
    public static class Customer {
        @Id String id;
        String legalName;
        int kycLevel;
        @ManyToOne @JoinColumn(name = "branch_code") Branch branch;
        Boolean pep;
    }

    @Entity @Table(name = "accounts")
    public static class Account {
        @Id String id;
        @ManyToOne Customer customer;
        String currency;
        String status;
        BigDecimal overdraftLimit;
    }

    @Entity @Table(name = "payments")
    public static class Payment {
        @Id String id;
        @ManyToOne Account account;
        BigDecimal amount;
        String currency;
        String status;
        Double riskScore;
        Instant createdAt;
        String cpName;
        String cpIban;
        String cpCountry;
        String initiatedBy;
        @ElementCollection
        @CollectionTable(name = "payment_flags", joinColumns = @JoinColumn(name = "payment_id"))
        @Column(name = "flag")
        Set<String> flags;
        @OneToMany(mappedBy = "payment") List<PaymentApproval> approvals;
    }

    @Entity @Table(name = "payment_approvals")
    public static class PaymentApproval {
        @Id String id;
        @ManyToOne Payment payment;
        String approverId;
        String decision;
    }

    public String domain() { return "fintech"; }

    public List<Class<?>> entities() {
        return List.of(Branch.class, Customer.class, Account.class, Payment.class, PaymentApproval.class);
    }

    public Class<?> root() { return Payment.class; }

    public Options options() {
        String a = "request.resource.attr.";
        return Options.of(Map.ofEntries(
                Map.entry(a + "amount", field("amount")),
                Map.entry(a + "currency", field("currency")),
                Map.entry(a + "status", field("status")),
                Map.entry(a + "riskScore", omitted("riskScore")),
                Map.entry(a + "createdAt", field("createdAt")),
                Map.entry(a + "initiatedBy", field("initiatedBy")),
                Map.entry(a + "counterparty.name", field("cpName")),
                Map.entry(a + "counterparty.iban", field("cpIban")),
                Map.entry(a + "counterparty.country", field("cpCountry")),
                Map.entry(a + "flags", relation("flags")),
                Map.entry(a + "approvals", relation("approvals", Map.of())),
                Map.entry(a + "account.currency", field("account.currency")),
                Map.entry(a + "account.status", field("account.status")),
                Map.entry(a + "account.overdraftLimit", omitted("account.overdraftLimit")),
                Map.entry(a + "account.customer.id", field("account.customer.id")),
                Map.entry(a + "account.customer.kycLevel", field("account.customer.kycLevel")),
                Map.entry(a + "account.customer.pep", omitted("account.customer.pep")),
                Map.entry(a + "account.customer.branch.code", field("account.customer.branch.code")),
                Map.entry(a + "account.customer.branch.country", field("account.customer.branch.country"))));
    }
}
