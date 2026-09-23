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
 * One element of the corpus's {@code aNumberList}, as a row of its own.
 *
 * <p>An entity rather than an {@code @ElementCollection}: Hibernate never persists a null element
 * of an element collection, and the corpus carries one (a6 {@code [null, 2]}) — a VALUE CEL
 * compares, which a dropped row would turn into a list that no longer holds it. A related row
 * with a NULL {@code list_element} column is that null element under the adapter's
 * scalar-projection convention, exactly as a NULL {@code tags.name} is for {@code tagNames}.
 */
@Entity
@Table(name = "resource_number_list")
public class NumberListElementEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk")
    private Long pk;

    @Column(name = "list_element")
    private Double element;

    @ManyToOne
    @JoinColumn(name = "resource_id")
    private ResourceEntity resource;

    public NumberListElementEntity() {}

    public NumberListElementEntity(Double element, ResourceEntity resource) {
        this.element = element;
        this.resource = resource;
    }

    public Long getPk() { return pk; }
    public Double getElement() { return element; }
    public void setElement(Double element) { this.element = element; }
    public ResourceEntity getResource() { return resource; }
    public void setResource(ResourceEntity resource) { this.resource = resource; }
}
