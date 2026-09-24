/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata.testmodel;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;

/**
 * First hop of the corpus's to-one relation ({@code parentSeedId} in
 * {@code conformance/seeds.json}). Unlike {@link NestedEmbeddable}, this is a separate table
 * reached through a join. The join column is unique, so no two resources share a parent row.
 */
@Entity
@Table(name = "adversarial_parent")
public class AdversarialParentEntity {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "a_bool")
    private Boolean aBool;

    @Column(name = "a_string")
    private String aString;

    @Column(name = "a_number")
    private Integer aNumber;

    @Column(name = "a_optional_string")
    private String aOptionalString;

    @OneToOne
    @JoinColumn(name = "resource_id", unique = true)
    private ResourceEntity resource;

    @OneToOne(mappedBy = "parent")
    private AdversarialInnerEntity inner;

    public AdversarialParentEntity() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public Boolean getaBool() { return aBool; }
    public void setaBool(Boolean aBool) { this.aBool = aBool; }
    public String getaString() { return aString; }
    public void setaString(String aString) { this.aString = aString; }
    public Integer getaNumber() { return aNumber; }
    public void setaNumber(Integer aNumber) { this.aNumber = aNumber; }
    public String getaOptionalString() { return aOptionalString; }
    public void setaOptionalString(String aOptionalString) { this.aOptionalString = aOptionalString; }
    public ResourceEntity getResource() { return resource; }
    public void setResource(ResourceEntity resource) { this.resource = resource; }
    public AdversarialInnerEntity getInner() { return inner; }
    public void setInner(AdversarialInnerEntity inner) { this.inner = inner; }
}
