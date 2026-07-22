package dev.cerbos.example.photos;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Table;

import java.util.LinkedHashSet;
import java.util.Set;

@Entity
@Table(name = "albums")
public class Album {

    @Id
    private String id;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "owner_id", nullable = false)
    private String ownerId;

    @Column(name = "title", nullable = false)
    private String title;

    @Column(name = "is_shared", nullable = false)
    private boolean shared;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "album_collaborators", joinColumns = @JoinColumn(name = "album_id"))
    @Column(name = "principal_id")
    private Set<String> collaborators = new LinkedHashSet<>();

    protected Album() {}

    public Album(String id, String tenantId, String ownerId, String title, boolean shared,
                 Set<String> collaborators) {
        this.id = id;
        this.tenantId = tenantId;
        this.ownerId = ownerId;
        this.title = title;
        this.shared = shared;
        this.collaborators = new LinkedHashSet<>(collaborators);
    }

    public String getId() { return id; }
    public String getTenantId() { return tenantId; }
    public String getOwnerId() { return ownerId; }
    public String getTitle() { return title; }
    public boolean isShared() { return shared; }
    public Set<String> getCollaborators() { return collaborators; }
}
