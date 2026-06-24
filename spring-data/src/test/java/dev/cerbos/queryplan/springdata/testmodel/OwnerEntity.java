package dev.cerbos.queryplan.springdata.testmodel;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * A simple owner/user entity used to demonstrate single-valued (@ManyToOne) relation traversal.
 * Cerbos plans that reference {@code request.resource.attr.creator.name} or similar dotted paths
 * are translated to {@code root.get("creator").get("name")} JPA paths — no special configuration
 * is required for one-to-one or many-to-one relations beyond the dotted field mapping.
 */
@Entity
@Table(name = "owners")
public class OwnerEntity {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "name")
    private String name;

    @Column(name = "department")
    private String department;

    public OwnerEntity() {}

    public OwnerEntity(String id, String name, String department) {
        this.id = id;
        this.name = name;
        this.department = department;
    }

    public String getId() { return id; }
    public String getName() { return name; }
    public String getDepartment() { return department; }
}
