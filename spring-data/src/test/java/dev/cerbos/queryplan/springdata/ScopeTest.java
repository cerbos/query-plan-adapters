/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.queryplan.springdata.testmodel.ResourceEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Root;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Which {@link Scope} owns a collection referenced inside a nested lambda. A wrong owner fails
 * silently, because an element entity can have a collection with the same name, and no corpus
 * case nests a root collection under an element that has one. Runs offline on H2.
 */
class ScopeTest {

    private static final String CATEGORIES = "request.resource.attr.categories";

    private static final AttributeMapping.Relation SUB_CATEGORIES =
            AttributeMapping.relation("subCategories", "name",
                    Map.of("name", AttributeMapping.field("name")));

    private static final Map<String, AttributeMapping> MAPPER = Map.of(
            CATEGORIES, AttributeMapping.relation("categories", Map.of(
                    "name", AttributeMapping.field("name"),
                    "subCategories", SUB_CATEGORIES)));

    private static EntityManagerFactory emf;

    @BeforeAll
    static void setUp() {
        emf = Persistence.createEntityManagerFactory("test-pu");
    }

    @AfterAll
    static void tearDown() {
        if (emf != null) emf.close();
    }

    /**
     * A sub-category also has a {@code categories} collection. Anchoring to the inner lambda
     * would still build a query, but over the wrong collection.
     */
    @Test
    void outerRelationIsNotCapturedByASameNamedCollectionOnTheElement() {
        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<ResourceEntity> query = cb.createQuery(ResourceEntity.class);
            Root<ResourceEntity> root = query.from(ResourceEntity.class);
            Scope rootScope = Scope.root(root, query, MAPPER);

            Join<?, ?> categoryJoin = root.join("categories");
            Scope lambdaScope = Scope.lambda(categoryJoin, query,
                    (AttributeMapping.Relation) MAPPER.get(CATEGORIES), "c", rootScope);
            Join<?, ?> subCategoryJoin = categoryJoin.join("subCategories");
            Scope subLambda = Scope.lambda(subCategoryJoin, query, SUB_CATEGORIES, "s", lambdaScope);

            Scope.ResolvedRelation rel = assertInstanceOf(Scope.ResolvedRelation.class,
                    subLambda.resolve(CATEGORIES));
            assertSame(rootScope, rel.owner());
            assertSame(root, rel.owner().from());
        } finally {
            em.close();
        }
    }
}
