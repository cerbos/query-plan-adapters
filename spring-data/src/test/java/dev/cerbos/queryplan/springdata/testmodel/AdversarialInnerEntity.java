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
 * Second hop of the corpus's to-one relation, {@code parent.inner}. Only some parents have one, so
 * a missing second hop is covered too.
 */
@Entity
@Table(name = "adversarial_inner")
public class AdversarialInnerEntity {

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
    @JoinColumn(name = "parent_id", unique = true)
    private AdversarialParentEntity parent;

    public AdversarialInnerEntity() {}

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
    public AdversarialParentEntity getParent() { return parent; }
    public void setParent(AdversarialParentEntity parent) { this.parent = parent; }
}
