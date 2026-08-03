package dev.cerbos.queryplan.springdata.testmodel;

import jakarta.persistence.CascadeType;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "resources")
public class ResourceEntity {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "oid")
    private String oid;

    @Column(name = "a_bool")
    private Boolean aBool;

    @Column(name = "a_string")
    private String aString;

    @Column(name = "a_number")
    private Integer aNumber;

    // Fractional-value column for the IEEE add-solve probes: the -0.6 reproduction needs a
    // stored double, which the Integer aNumber column cannot hold.
    @Column(name = "a_double")
    private Double aDouble;

    @Column(name = "a_optional_string")
    private String aOptionalString;

    @Column(name = "created_by")
    private String createdBy;

    // Temporal columns for the timestamp() comparison support: Instant and OffsetDateTime are
    // the two column types the adapter translates (both unambiguously denote an absolute
    // instant); localCreatedAt exists to pin the named error for ambiguous temporal types.
    @Column(name = "created_at")
    private Instant createdAt;

    @Column(name = "updated_at")
    private OffsetDateTime updatedAt;

    @Column(name = "local_created_at")
    private LocalDateTime localCreatedAt;

    @Column(name = "scope")
    private String scope;

    @ElementCollection
    @CollectionTable(name = "resource_owned_by", joinColumns = @JoinColumn(name = "resource_id"))
    @Column(name = "owner")
    private List<String> ownedBy = new ArrayList<>();

    @ElementCollection
    @CollectionTable(name = "resource_tag_names", joinColumns = @JoinColumn(name = "resource_id"))
    @Column(name = "tag_name")
    private List<String> tagNames = new ArrayList<>();

    @OneToMany(mappedBy = "resource", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<TagEntity> tags = new ArrayList<>();

    @ManyToMany
    @JoinTable(name = "resource_category",
            joinColumns = @JoinColumn(name = "resource_id"),
            inverseJoinColumns = @JoinColumn(name = "category_id"))
    private List<CategoryEntity> categories = new ArrayList<>();

    @ManyToOne
    @JoinColumn(name = "creator_id")
    private OwnerEntity creator;

    @Embedded
    private NestedEmbeddable nested;

    public ResourceEntity() {}

    public ResourceEntity(String id) {
        this.id = id;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getOid() { return oid; }
    public void setOid(String oid) { this.oid = oid; }
    public Boolean getaBool() { return aBool; }
    public void setaBool(Boolean aBool) { this.aBool = aBool; }
    public String getaString() { return aString; }
    public void setaString(String aString) { this.aString = aString; }
    public Integer getaNumber() { return aNumber; }
    public void setaNumber(Integer aNumber) { this.aNumber = aNumber; }
    public Double getaDouble() { return aDouble; }
    public void setaDouble(Double aDouble) { this.aDouble = aDouble; }
    public String getaOptionalString() { return aOptionalString; }
    public void setaOptionalString(String aOptionalString) { this.aOptionalString = aOptionalString; }
    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public OffsetDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(OffsetDateTime updatedAt) { this.updatedAt = updatedAt; }
    public LocalDateTime getLocalCreatedAt() { return localCreatedAt; }
    public void setLocalCreatedAt(LocalDateTime localCreatedAt) { this.localCreatedAt = localCreatedAt; }
    public String getScope() { return scope; }
    public void setScope(String scope) { this.scope = scope; }
    public List<String> getOwnedBy() { return ownedBy; }
    public void setOwnedBy(List<String> ownedBy) { this.ownedBy = ownedBy; }
    public List<String> getTagNames() { return tagNames; }
    public void setTagNames(List<String> tagNames) { this.tagNames = tagNames; }
    public List<TagEntity> getTags() { return tags; }
    public void setTags(List<TagEntity> tags) { this.tags = tags; }
    public List<CategoryEntity> getCategories() { return categories; }
    public void setCategories(List<CategoryEntity> categories) { this.categories = categories; }
    public OwnerEntity getCreator() { return creator; }
    public void setCreator(OwnerEntity creator) { this.creator = creator; }
    public NestedEmbeddable getNested() { return nested; }
    public void setNested(NestedEmbeddable nested) { this.nested = nested; }

    public ResourceEntity addTag(String tagId, String tagName) {
        TagEntity t = new TagEntity(tagId, tagName, this);
        tags.add(t);
        return this;
    }
}
