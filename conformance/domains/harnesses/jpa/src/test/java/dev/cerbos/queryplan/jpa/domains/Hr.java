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

class Hr implements DomainMapping {

    @Entity @Table(name = "departments")
    public static class Department {
        @Id String id;
        String name;
        String costCenter;
    }

    @Entity @Table(name = "locations")
    public static class Location {
        @Id String code;
        String country;
        String city;
    }

    @Entity @Table(name = "employees")
    public static class Employee {
        @Id String id;
        @ManyToOne Employee manager;
        @ManyToOne Department department;
        @ManyToOne @JoinColumn(name = "location_code") Location location;
        int level;
        String employmentType;
        String salaryBand;
        Instant hiredOn;
        Instant terminatedOn;
        String workEmail;
        @ElementCollection
        @CollectionTable(name = "employee_skills", joinColumns = @JoinColumn(name = "employee_id"))
        @Column(name = "skill")
        Set<String> skills;
    }

    public String domain() { return "hr"; }

    public List<Class<?>> entities() {
        return List.of(Department.class, Location.class, Employee.class);
    }

    public Class<?> root() { return Employee.class; }

    public Options options() {
        String a = "request.resource.attr.";
        return Options.of(Map.ofEntries(
                Map.entry("request.resource.id", field("id")),
                Map.entry(a + "level", field("level")),
                Map.entry(a + "employmentType", field("employmentType")),
                Map.entry(a + "salaryBand", omitted("salaryBand")),
                Map.entry(a + "hiredOn", field("hiredOn")),
                Map.entry(a + "terminatedOn", omitted("terminatedOn")),
                Map.entry(a + "email", field("workEmail")),
                Map.entry(a + "skills", relation("skills")),
                Map.entry(a + "department.id", field("department.id")),
                Map.entry(a + "department.costCenter", field("department.costCenter")),
                Map.entry(a + "location.country", field("location.country")),
                Map.entry(a + "location.city", field("location.city")),
                Map.entry(a + "manager.id", field("manager.id")),
                Map.entry(a + "manager.manager.id", field("manager.manager.id"))));
    }
}
