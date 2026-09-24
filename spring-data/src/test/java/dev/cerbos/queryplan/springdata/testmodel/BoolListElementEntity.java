/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata.testmodel;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

/**
 * One element of the corpus's {@code aBoolList}. An entity, not an {@code @ElementCollection},
 * because the corpus has a null element and Hibernate drops nulls from element collections.
 */
@Entity
@Table(name = "resource_bool_list")
public class BoolListElementEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk")
    private Long pk;

    @Column(name = "list_element")
    private Boolean element;

    @ManyToOne
    @JoinColumn(name = "resource_id")
    private ResourceEntity resource;

    /** The element's index in the list the corpus sends to check(). */
    @Column(name = "list_position")
    private Integer position;

    public BoolListElementEntity() {}

    public BoolListElementEntity(Boolean element, ResourceEntity resource) {
        this.element = element;
        this.resource = resource;
    }

    public Long getPk() { return pk; }
    public Boolean getElement() { return element; }
    public void setElement(Boolean element) { this.element = element; }
    public ResourceEntity getResource() { return resource; }
    public Integer getPosition() { return position; }
    public void setPosition(Integer position) { this.position = position; }
    public void setResource(ResourceEntity resource) { this.resource = resource; }
}
