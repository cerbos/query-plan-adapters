package dev.cerbos.example.photos;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Table;

import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "photos")
public class Photo {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "owner_id", nullable = false)
    private String ownerId;

    @Column(name = "title", nullable = false)
    private String title;

    @Column(name = "is_public", nullable = false)
    private boolean isPublic;

    @Column(name = "is_archived", nullable = false)
    private boolean isArchived;

    @Column(name = "location")
    private String location;

    // EAGER so the controller can serialize tags after the @Transactional repository call —
    // the example uses spring.jpa.open-in-view=false to avoid the lazy-init footgun.
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "photo_tags", joinColumns = @JoinColumn(name = "photo_id"))
    @Column(name = "tag")
    private Set<String> tags = new HashSet<>();

    public Photo() {}

    public Photo(String id, String ownerId, String title, boolean isPublic, boolean isArchived,
                 String location, Set<String> tags) {
        this.id = id;
        this.ownerId = ownerId;
        this.title = title;
        this.isPublic = isPublic;
        this.isArchived = isArchived;
        this.location = location;
        this.tags = tags;
    }

    public String getId() { return id; }
    public String getOwnerId() { return ownerId; }
    public String getTitle() { return title; }
    public boolean isPublic() { return isPublic; }
    public boolean isArchived() { return isArchived; }
    public String getLocation() { return location; }
    public Set<String> getTags() { return tags; }
}
