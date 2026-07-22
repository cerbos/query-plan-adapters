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

    public String getId() { return id; }
    public String getTenantId() { return tenantId; }
    public String getOwnerId() { return ownerId; }
    public String getTitle() { return title; }
    public boolean isPublic() { return isPublic; }
    public boolean isArchived() { return isArchived; }
    public String getLocation() { return location; }
    public int getRating() { return rating; }
    public PhotoDetails getDetails() { return details; }
    public Set<String> getTags() { return tags; }
    public Set<PhotoLabel> getLabels() { return labels; }
}
