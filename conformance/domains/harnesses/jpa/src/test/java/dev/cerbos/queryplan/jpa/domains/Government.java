package dev.cerbos.queryplan.jpa.domains;

import static dev.cerbos.queryplan.jpa.AttributeMapping.field;
import static dev.cerbos.queryplan.jpa.AttributeMapping.relation;
import static dev.cerbos.queryplan.jpa.domains.DomainMapping.explicit;

import dev.cerbos.queryplan.jpa.JpaQueryPlanAdapter.Options;
import dev.cerbos.queryplan.jpa.NullAttributeRepresentation;

import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

class Government implements DomainMapping {

    @Entity @Table(name = "agencies")
    public static class Agency {
        @Id String code;
        String name;
        String state;
    }

    @Entity @Table(name = "applicants")
    public static class Applicant {
        @Id String id;
        String countyFips;
        int householdSize;
        String citizenship;
    }

    @Entity @Table(name = "cases")
    public static class BenefitCase {
        @Id String id;
        String caseNumber;
        @ManyToOne @JoinColumn(name = "agency_code") Agency agency;
        @ManyToOne Applicant applicant;
        String status;
        int clearanceLevel;
        String program;
        Instant openedOn;
        Instant closedOn;
        boolean pii;
        String assignedTo;
        BigDecimal monthlyAmount;
        @ElementCollection
        @CollectionTable(name = "case_flags", joinColumns = @JoinColumn(name = "case_id"))
        @Column(name = "flag")
        Set<String> flags;
    }

    public String domain() { return "government"; }

    public List<Class<?>> entities() {
        return List.of(Agency.class, Applicant.class, BenefitCase.class);
    }

    public Class<?> root() { return BenefitCase.class; }

    public Options options() {
        String a = "request.resource.attr.";
        return Options.of(Map.ofEntries(
                Map.entry(a + "caseNumber", field("caseNumber")),
                Map.entry(a + "status", field("status")),
                Map.entry(a + "clearanceLevel", field("clearanceLevel")),
                Map.entry(a + "program", field("program")),
                Map.entry(a + "openedOn", field("openedOn")),
                Map.entry(a + "closedOn", explicit("closedOn")),
                Map.entry(a + "pii", field("pii")),
                Map.entry(a + "assignedTo", explicit("assignedTo")),
                Map.entry(a + "monthlyAmount", explicit("monthlyAmount")),
                Map.entry(a + "flags", relation("flags")),
                Map.entry(a + "agency.code", field("agency.code")),
                Map.entry(a + "agency.state", field("agency.state")),
                Map.entry(a + "applicant.countyFips", field("applicant.countyFips")),
                Map.entry(a + "applicant.householdSize", field("applicant.householdSize")),
                Map.entry(a + "applicant.citizenship", explicit("applicant.citizenship")))
        ).withNullAttributeRepresentation(NullAttributeRepresentation.EXPLICIT);
    }
}
