/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata.testmodel;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** Target of the {@code @ManyToOne} {@code creator} relation, reached with a dotted field path. */
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
