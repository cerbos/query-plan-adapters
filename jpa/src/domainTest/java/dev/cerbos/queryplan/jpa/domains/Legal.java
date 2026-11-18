package dev.cerbos.queryplan.jpa.domains;

import static dev.cerbos.queryplan.jpa.AttributeMapping.field;
import static dev.cerbos.queryplan.jpa.AttributeMapping.relation;
import static dev.cerbos.queryplan.jpa.domains.DomainMapping.explicit;

import dev.cerbos.queryplan.jpa.JpaQueryPlanAdapter.Options;
import dev.cerbos.queryplan.jpa.NullAttributeRepresentation;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

class Legal implements DomainMapping {

    @Entity @Table(name = "clients")
    public static class Client {
        @Id String id;
        String name;
        String industry;
    }

    @Entity @Table(name = "matters")
    public static class Matter {
        @Id String id;
        @ManyToOne Client client;
        String practiceArea;
        String status;
        Instant openedAt;
        String leadPartnerId;
        @ElementCollection
        @CollectionTable(name = "matter_team", joinColumns = @JoinColumn(name = "matter_id"))
        @Column(name = "user_id")
        Set<String> team;
        @ElementCollection
        @CollectionTable(name = "matter_screens", joinColumns = @JoinColumn(name = "matter_id"))
        @Column(name = "user_id")
        Set<String> screened;
    }

    @Entity @Table(name = "documents")
    public static class Document {
        @Id String id;
        @ManyToOne Matter matter;
        String title;
        String privilege;
        int classification;
        boolean legalHold;
        String createdBy;
        Instant retentionUntil;
        @OneToMany(mappedBy = "document") List<DocumentShare> shares;
    }

    @Entity @Table(name = "document_shares")
    public static class DocumentShare {
        @Id String id;
        @ManyToOne Document document;
        String partyType;
        String partyId;
    }

    public String domain() { return "legal"; }

    public List<Class<?>> entities() {
        return List.of(Client.class, Matter.class, Document.class, DocumentShare.class);
    }

    public Class<?> root() { return Document.class; }

    public Options options() {
        String a = "request.resource.attr.";
        return Options.of(Map.ofEntries(
                Map.entry(a + "title", field("title")),
                Map.entry(a + "privilege", explicit("privilege")),
                Map.entry(a + "classification", field("classification")),
                Map.entry(a + "legalHold", field("legalHold")),
                Map.entry(a + "createdBy", field("createdBy")),
                Map.entry(a + "retentionUntil", explicit("retentionUntil")),
                Map.entry(a + "shares", relation("shares", Map.of())),
                Map.entry(a + "matter.status", field("matter.status")),
                Map.entry(a + "matter.practiceArea", field("matter.practiceArea")),
                Map.entry(a + "matter.leadPartnerId", explicit("matter.leadPartnerId")),
                Map.entry(a + "matter.client.id", field("matter.client.id")),
                Map.entry(a + "matter.client.industry", explicit("matter.client.industry")),
                // Lists on the to-one matter: the hop is a relation so the chain can reach them.
                Map.entry(a + "matter", relation("matter", Map.of(
                        "team", relation("team"), "screened", relation("screened")))))
        ).withNullAttributeRepresentation(NullAttributeRepresentation.EXPLICIT);
    }
}
