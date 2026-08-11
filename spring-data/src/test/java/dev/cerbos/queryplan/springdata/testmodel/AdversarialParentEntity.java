package dev.cerbos.queryplan.springdata.testmodel;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;

/**
 * The first level of the conformance corpus's one REAL to-one relation
 * ({@code conformance/seeds.json}'s {@code parentSeedId}).
 *
 * <p>Unlike {@link NestedEmbeddable}, which is an {@code @Embedded} value living in the resource's
 * own row, this is a separate table reached through a join — which is the whole point: the corpus
 * already carries a dotted attribute that is a flat column wearing a dotted name, and this is the
 * one that is not. A resource owns its own parent chain (the join column is unique, so the
 * relation is to-ONE), so no two resources share a parent row and a filter that returned the
 * parent instead of the child could not agree with the oracle by accident.
 */
@Entity
@Table(name = "adversarial_parent")
public class AdversarialParentEntity {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "a_bool")
    private Boolean aBool;

    @Column(name = "a_string")
    private String aString;

    @Column(name = "a_number")
    private Integer aNumber;

    @Column(name = "a_optional_string")
    private String aOptionalString;

    @OneToOne
    @JoinColumn(name = "resource_id", unique = true)
    private ResourceEntity resource;

    @OneToOne(mappedBy = "parent")
    private AdversarialInnerEntity inner;

    public AdversarialParentEntity() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public Boolean getaBool() { return aBool; }
    public void setaBool(Boolean aBool) { this.aBool = aBool; }
    public String getaString() { return aString; }
    public void setaString(String aString) { this.aString = aString; }
    public Integer getaNumber() { return aNumber; }
    public void setaNumber(Integer aNumber) { this.aNumber = aNumber; }
    public String getaOptionalString() { return aOptionalString; }
    public void setaOptionalString(String aOptionalString) { this.aOptionalString = aOptionalString; }
    public ResourceEntity getResource() { return resource; }
    public void setResource(ResourceEntity resource) { this.resource = resource; }
    public AdversarialInnerEntity getInner() { return inner; }
    public void setInner(AdversarialInnerEntity inner) { this.inner = inner; }
}
