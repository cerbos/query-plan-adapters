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

@Entity
@Table(name = "tags")
public class TagEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk")
    private Long pk;

    @Column(name = "tag_id")
    private String id;

    @Column(name = "name")
    private String name;

    @ManyToOne
    @JoinColumn(name = "resource_id")
    private ResourceEntity resource;

    /** The element's index in the list the corpus sends to check(). */
    @Column(name = "list_position")
    private Integer position;

    public TagEntity() {}

    public TagEntity(String id, String name, ResourceEntity resource) {
        this.id = id;
        this.name = name;
        this.resource = resource;
    }

    public Long getPk() { return pk; }
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public ResourceEntity getResource() { return resource; }
    public Integer getPosition() { return position; }
    public void setPosition(Integer position) { this.position = position; }
    public void setResource(ResourceEntity resource) { this.resource = resource; }
}
