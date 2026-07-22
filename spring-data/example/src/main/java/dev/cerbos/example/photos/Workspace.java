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
@Table(name = "workspaces")
public class Workspace {

    @Id
    private String id;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "owner_id", nullable = false)
    private String ownerId;

    @Column(name = "name", nullable = false)
    private String name;

    @Column(name = "is_active", nullable = false)
    private boolean active;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "workspace_members", joinColumns = @JoinColumn(name = "workspace_id"))
    @Column(name = "principal_id")
    private Set<String> members = new LinkedHashSet<>();

    protected Workspace() {}

    public Workspace(String id, String tenantId, String ownerId, String name, boolean active,
                     Set<String> members) {
        this.id = id;
        this.tenantId = tenantId;
        this.ownerId = ownerId;
        this.name = name;
        this.active = active;
        this.members = new LinkedHashSet<>(members);
    }

    public String getId() { return id; }
    public String getTenantId() { return tenantId; }
    public String getOwnerId() { return ownerId; }
    public String getName() { return name; }
    public boolean isActive() { return active; }
    public Set<String> getMembers() { return members; }
}
