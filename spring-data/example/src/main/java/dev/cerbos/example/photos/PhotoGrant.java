package dev.cerbos.example.photos;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

@Entity
@Table(name = "photo_grants")
public class PhotoGrant {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "permission", nullable = false)
    private String permission;

    @Column(name = "user_id")
    private String userId;

    @Column(name = "group_id")
    private String groupId;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "photo_id", nullable = false)
    private Photo photo;

    protected PhotoGrant() {}

    PhotoGrant(Photo photo, String tenantId, String permission, String userId, String groupId) {
        this.photo = photo;
        this.tenantId = tenantId;
        this.permission = permission;
        this.userId = userId;
        this.groupId = groupId;
    }
}
