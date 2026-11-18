package dev.cerbos.queryplan.jpa.domains;

import static dev.cerbos.queryplan.jpa.AttributeMapping.field;
import static dev.cerbos.queryplan.jpa.AttributeMapping.relation;
import static dev.cerbos.queryplan.jpa.domains.DomainMapping.omitted;

import dev.cerbos.queryplan.jpa.JpaQueryPlanAdapter.Options;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

class Healthcare implements DomainMapping {

    @Entity @Table(name = "hospitals")
    public static class Hospital {
        @Id String id;
        String name;
        String region;
    }

    @Entity @Table(name = "wards")
    public static class Ward {
        @Id String id;
        String name;
        @ManyToOne Hospital hospital;
        boolean secure;
    }

    @Entity @Table(name = "patients")
    public static class Patient {
        @Id String id;
        String mrn;
        @ManyToOne Ward ward;
        boolean consentResearch;
        Boolean vip;
        int birthYear;
    }

    @Entity @Table(name = "clinical_notes")
    public static class ClinicalNote {
        @Id String noteId;
        @ManyToOne Patient patient;
        String authorId;
        @Column(name = "type") String noteType;
        Integer sensitivity;
        Instant signedAt;
        @Column(name = "is_deleted") boolean deleted;
        @ElementCollection
        @CollectionTable(name = "note_care_team", joinColumns = @JoinColumn(name = "note_id"))
        @Column(name = "clinician_id")
        Set<String> careTeam;
        @OneToMany(mappedBy = "note") List<NoteCode> codes;
    }

    @Entity @Table(name = "note_codes")
    public static class NoteCode {
        @Id String id;
        @ManyToOne @JoinColumn(name = "note_id") ClinicalNote note;
        String system;
        String code;
    }

    public String domain() { return "healthcare"; }

    public List<Class<?>> entities() {
        return List.of(Hospital.class, Ward.class, Patient.class, ClinicalNote.class, NoteCode.class);
    }

    public Class<?> root() { return ClinicalNote.class; }

    public String idAttribute() { return "noteId"; }

    public Options options() {
        String a = "request.resource.attr.";
        return Options.of(Map.ofEntries(
                Map.entry(a + "noteType", field("noteType")),
                Map.entry(a + "authorId", field("authorId")),
                Map.entry(a + "sensitivity", omitted("sensitivity")),
                Map.entry(a + "signedAt", omitted("signedAt")),
                Map.entry(a + "deleted", field("deleted")),
                Map.entry(a + "careTeam", relation("careTeam")),
                Map.entry(a + "codes", relation("codes", Map.of())),
                Map.entry(a + "patient.mrn", field("patient.mrn")),
                Map.entry(a + "patient.consentResearch", field("patient.consentResearch")),
                Map.entry(a + "patient.vip", omitted("patient.vip")),
                Map.entry(a + "patient.birthYear", field("patient.birthYear")),
                Map.entry(a + "patient.ward.id", field("patient.ward.id")),
                Map.entry(a + "patient.ward.secure", field("patient.ward.secure")),
                Map.entry(a + "patient.ward.hospital.region", field("patient.ward.hospital.region"))));
    }
}
