/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.example.demo;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * A document from {@code demo/seeds.json}.
 *
 * <p>The policy never reads {@code region} or {@code archived}. They belong to the application's
 * own filter, which usage shape 5 composes with the adapter's Specification (see
 * {@code DemoShapes}).
 *
 * <p>It is kept flat, with no relations, nullable columns or LIKE metacharacters. The conformance
 * corpus covers those.
 */
@Entity
@Table(name = "documents")
public class DemoDocument {

    @Id
    private String id;

    @Column(name = "owner_id", nullable = false)
    private String ownerId;

    // The policy calls this attribute "public", a Java keyword. DemoShapes maps it to this field.
    @Column(name = "is_public", nullable = false)
    private boolean isPublic;

    @Column(name = "region", nullable = false)
    private String region;

    @Column(name = "archived", nullable = false)
    private boolean archived;

    protected DemoDocument() {}

    public DemoDocument(String id, String ownerId, boolean isPublic, String region,
                        boolean archived) {
        this.id = id;
        this.ownerId = ownerId;
        this.isPublic = isPublic;
        this.region = region;
        this.archived = archived;
    }

    public String getId() { return id; }
    public String getOwnerId() { return ownerId; }
    public boolean isPublic() { return isPublic; }
    public String getRegion() { return region; }
    public boolean isArchived() { return archived; }
}
