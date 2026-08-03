package dev.cerbos.example.photos;

import jakarta.persistence.CascadeType;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Entity
@Table(name = "photos")
public class Photo {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "owner_id", nullable = false)
    private String ownerId;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "title", nullable = false)
    private String title;

    @Column(name = "is_public", nullable = false)
    private boolean isPublic;

    @Column(name = "is_archived", nullable = false)
    private boolean isArchived;

    @Column(name = "location")
    private String location;

    @Column(name = "rating", nullable = false)
    private int rating;

    /**
     * Nullable IEEE-double attribute used only by the {@code edge-ieee-*} regression
     * scenarios (see scripts/smoke-edge-cases.sh). Kept nullable so the regular fixtures
     * are unaffected: a NULL score is UNKNOWN under SQL three-valued logic and a missing
     * attribute is a CEL evaluation error at check() time — both sides exclude the row.
     */
    @Column(name = "score")
    private Double score;

    /**
     * Photo creation instant for the {@code edge-retention} time-window scenario. Mapped as
     * {@link Instant} because the adapter's {@code timestamp()} support compares folded
     * RFC-3339 plan constants against {@code Instant}/{@code OffsetDateTime} columns.
     */
    @Column(name = "created_at")
    private Instant createdAt;

    @Embedded
    private PhotoDetails details;

    // EAGER so the controller can build DTOs after the repository transaction has closed —
    // the example uses spring.jpa.open-in-view=false to avoid hidden persistence work in the web layer.
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "photo_tags", joinColumns = @JoinColumn(name = "photo_id"))
    @Column(name = "tag")
    private Set<String> tags = new LinkedHashSet<>();

    @OneToMany(mappedBy = "photo", cascade = CascadeType.ALL, orphanRemoval = true,
            fetch = FetchType.EAGER)
    private Set<PhotoLabel> labels = new LinkedHashSet<>();

    @OneToMany(mappedBy = "photo", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<PhotoGrant> grants = new ArrayList<>();

    public Photo() {}

    public Photo(String id, String tenantId, String ownerId, String title, boolean isPublic,
                 boolean isArchived, String location, int rating, PhotoDetails details,
                 Set<String> tags) {
        this.id = id;
        this.tenantId = tenantId;
        this.ownerId = ownerId;
        this.title = title;
        this.isPublic = isPublic;
        this.isArchived = isArchived;
        this.location = location;
        this.rating = rating;
        this.details = details;
        this.tags = tags;
    }

    public Photo addLabel(String name, double confidence, boolean reviewed) {
        labels.add(new PhotoLabel(this, name, confidence, reviewed));
        return this;
    }

    public Photo addGrant(String tenantId, String permission, String userId, String groupId) {
        grants.add(new PhotoGrant(this, tenantId, permission, userId, groupId));
        return this;
    }

    /** Fluent setter used only by the edge-case regression fixtures. */
    public Photo withScore(Double score) {
        this.score = score;
        return this;
    }

    /** Fluent setter used only by the edge-case regression fixtures. */
    public Photo withCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
        return this;
    }

    public String getId() { return id; }
    public String getTenantId() { return tenantId; }
    public String getOwnerId() { return ownerId; }
    public String getTitle() { return title; }
    public boolean isPublic() { return isPublic; }
    public boolean isArchived() { return isArchived; }
    public String getLocation() { return location; }
    public int getRating() { return rating; }
    public Double getScore() { return score; }
    public Instant getCreatedAt() { return createdAt; }
    public PhotoDetails getDetails() { return details; }
    public Set<String> getTags() { return tags; }
    public Set<PhotoLabel> getLabels() { return labels; }
}
