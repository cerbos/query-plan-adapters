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
 * One element of the corpus's {@code aNumberList}. An entity, not an {@code @ElementCollection},
 * because the corpus has a null element and Hibernate drops nulls from element collections. A
 * NULL {@code list_element} is that null element.
 */
@Entity
@Table(name = "resource_number_list")
public class NumberListElementEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk")
    private Long pk;

    @Column(name = "list_element")
    private Double element;

    @ManyToOne
    @JoinColumn(name = "resource_id")
    private ResourceEntity resource;

    public NumberListElementEntity() {}

    public NumberListElementEntity(Double element, ResourceEntity resource) {
        this.element = element;
        this.resource = resource;
    }

    public Long getPk() { return pk; }
    public Double getElement() { return element; }
    public void setElement(Double element) { this.element = element; }
    public ResourceEntity getResource() { return resource; }
    public void setResource(ResourceEntity resource) { this.resource = resource; }
}
