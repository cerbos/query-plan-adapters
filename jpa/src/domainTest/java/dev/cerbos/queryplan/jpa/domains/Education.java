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

class Education implements DomainMapping {

    @Entity @Table(name = "departments")
    public static class Department {
        @Id String id;
        String name;
    }

    @Entity @Table(name = "courses")
    public static class Course {
        @Id String id;
        String code;
        String term;
        @ManyToOne Department department;
        @OneToMany(mappedBy = "course") List<CourseStaff> staff;
    }

    @Entity @Table(name = "course_staff")
    public static class CourseStaff {
        @Id String id;
        @ManyToOne Course course;
        String userId;
        String role;
    }

    @Entity @Table(name = "assignments")
    public static class Assignment {
        @Id String id;
        @ManyToOne Course course;
        String title;
        Instant dueAt;
        int maxPoints;
        boolean groupWork;
    }

    @Entity @Table(name = "submissions")
    public static class Submission {
        @Id String id;
        @ManyToOne Assignment assignment;
        String studentId;
        Instant submittedAt;
        Double score;
        boolean late;
        Double plagiarismScore;
        String status;
        boolean locked;
        @ElementCollection
        @CollectionTable(name = "submission_members", joinColumns = @JoinColumn(name = "submission_id"))
        @Column(name = "student_id")
        Set<String> members;
    }

    public String domain() { return "education"; }

    public List<Class<?>> entities() {
        return List.of(Department.class, Course.class, CourseStaff.class, Assignment.class, Submission.class);
    }

    public Class<?> root() { return Submission.class; }

    public Options options() {
        String a = "request.resource.attr.";
        return Options.of(Map.ofEntries(
                Map.entry(a + "studentId", field("studentId")),
                Map.entry(a + "submittedAt", omitted("submittedAt")),
                Map.entry(a + "score", omitted("score")),
                Map.entry(a + "late", field("late")),
                Map.entry(a + "plagiarismScore", omitted("plagiarismScore")),
                Map.entry(a + "status", field("status")),
                Map.entry(a + "locked", field("locked")),
                Map.entry(a + "members", relation("members")),
                Map.entry(a + "assignment.dueAt", field("assignment.dueAt")),
                Map.entry(a + "assignment.maxPoints", field("assignment.maxPoints")),
                Map.entry(a + "assignment.groupWork", field("assignment.groupWork")),
                Map.entry(a + "assignment.course.code", field("assignment.course.code")),
                Map.entry(a + "assignment.course.term", field("assignment.course.term")),
                Map.entry(a + "assignment.course.department.id", field("assignment.course.department.id")),
                // A to-many two to-one hops below the root: the to-one hops are relations so the chain can reach it.
                Map.entry(a + "assignment", relation("assignment", Map.of(
                        "course", relation("course", Map.of("staff", relation("staff", Map.of()))))))));
    }
}
