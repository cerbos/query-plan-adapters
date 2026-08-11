package dev.cerbos.example.demo;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * The shared demo domain's one resource kind, as a JPA entity. One row per entry in
 * {@code demo/seeds.json}; the columns are exactly its four flat scalar attributes.
 *
 * <p>{@code region} and {@code archived} are never named by {@code demo/policies/document.yaml}.
 * They are the APPLICATION's own columns, and composing a predicate over them with the
 * adapter-produced Specification is usage shape 5 — see {@link DemoApplication}.
 *
 * <p>Deliberately flat: no relations, no nullable columns, no LIKE metacharacters. Each of those
 * divides the ten adapters, and proving them is the conformance corpus's job
 * ({@code ../src/test/java/.../AdversarialConformanceTest.java}), not this program's. The
 * photo/album/workspace application next door is where this example's own richer shapes live.
 */
@Entity
@Table(name = "documents")
public class DemoDocument {

    @Id
    private String id;

    @Column(name = "owner_id", nullable = false)
    private String ownerId;

    /**
     * {@code public} is a Java keyword, so the field carries the ORM-idiomatic name and
     * {@link DemoApplication}'s attribute mapping is what bridges it to the policy's
     * {@code request.resource.attr.public}. Mapping names to model names is the one piece of
     * configuration a consumer of this adapter cannot skip.
     */
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
