package dev.cerbos.queryplan.springdata.testmodel;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.Table;

import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "categories")
public class CategoryEntity {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "name")
    private String name;

    @ManyToMany(mappedBy = "categories")
    private List<ResourceEntity> resources = new ArrayList<>();

    @ManyToMany
    @JoinTable(name = "category_subcategory",
            joinColumns = @JoinColumn(name = "category_id"),
            inverseJoinColumns = @JoinColumn(name = "subcategory_id"))
    private List<SubCategoryEntity> subCategories = new ArrayList<>();

    public CategoryEntity() {}

    public CategoryEntity(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getId() { return id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public List<ResourceEntity> getResources() { return resources; }
    public List<SubCategoryEntity> getSubCategories() { return subCategories; }
    public void setSubCategories(List<SubCategoryEntity> subCategories) { this.subCategories = subCategories; }
}
