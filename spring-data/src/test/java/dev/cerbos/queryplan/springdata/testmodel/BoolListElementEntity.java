package dev.cerbos.queryplan.springdata.testmodel;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

/**
 * One element of the corpus's {@code aBoolList}, as a row of its own — an entity rather than an
 * {@code @ElementCollection} for the reason {@link NumberListElementEntity} gives: the corpus
 * carries a null element (a4 {@code [null, true]}), and Hibernate drops one from an element
 * collection.
 */
@Entity
@Table(name = "resource_bool_list")
public class BoolListElementEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk")
    private Long pk;

    @Column(name = "list_element")
    private Boolean element;

    @ManyToOne
    @JoinColumn(name = "resource_id")
    private ResourceEntity resource;

    public BoolListElementEntity() {}

    public BoolListElementEntity(Boolean element, ResourceEntity resource) {
        this.element = element;
        this.resource = resource;
    }

    public Long getPk() { return pk; }
    public Boolean getElement() { return element; }
    public void setElement(Boolean element) { this.element = element; }
    public ResourceEntity getResource() { return resource; }
    public void setResource(ResourceEntity resource) { this.resource = resource; }
}
