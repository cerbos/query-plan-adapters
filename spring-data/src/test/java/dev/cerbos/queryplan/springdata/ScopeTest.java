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
import jakarta.persistence.criteria.From;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Root;
import jakarta.persistence.criteria.Subquery;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link Scope}: whether a variable is a column or a collection, which scope owns a
 * collection, and how {@link Scope#rebaseAt} re-roots a scope for a correlated subquery. A wrong
 * owner fails silently, because an element entity can have a collection with the same name.
 * Runs offline on H2.
 */
class ScopeTest {

    private static final String TAGS = "request.resource.attr.tags";
    private static final String CATEGORIES = "request.resource.attr.categories";

    private static final AttributeMapping.Relation SUB_CATEGORIES =
            AttributeMapping.relation("subCategories", "name",
                    Map.of("name", AttributeMapping.field("name")));

    private static final Map<String, AttributeMapping> MAPPER = Map.of(
            "request.resource.attr.aString", AttributeMapping.field("aString"),
            TAGS, AttributeMapping.relation("tags", Map.of(
                    "name", AttributeMapping.field("name"))),
            CATEGORIES, AttributeMapping.relation("categories", Map.of(
                    "name", AttributeMapping.field("name"),
                    "subCategories", SUB_CATEGORIES)));

    private static EntityManagerFactory emf;

    private EntityManager em;
    private CriteriaBuilder cb;
    private CriteriaQuery<ResourceEntity> query;
    private Root<ResourceEntity> root;
    private Scope rootScope;

    @BeforeAll
    static void setUp() {
        emf = Persistence.createEntityManagerFactory("test-pu");
    }

    @AfterAll
    static void tearDown() {
        if (emf != null) emf.close();
    }

    @BeforeEach
    void newQuery() {
        em = emf.createEntityManager();
        cb = em.getCriteriaBuilder();
        query = cb.createQuery(ResourceEntity.class);
        root = query.from(ResourceEntity.class);
        rootScope = Scope.root(root, query, MAPPER);
    }

    @AfterEach
    void closeEm() {
        em.close();
    }

    /** The scope {@code categories.exists(c, ...)} builds. */
    private Scope categoriesLambda(Scope outer, From<?, ?> from) {
        return Scope.lambda(from, query,
                (AttributeMapping.Relation) MAPPER.get(CATEGORIES), "c", outer);
    }

    @Nested
    class RootResolution {

        @Test
        void fieldResolvesToItsColumnAndItsMapping() {
            Scope.ResolvedScalar scalar = assertInstanceOf(Scope.ResolvedScalar.class,
                    rootScope.resolve("request.resource.attr.aString"));
            assertSame(root, scalar.path().getParentPath());
            assertEquals(AttributeMapping.field("aString"), scalar.mapping());
        }

        @Test
        void relationResolvesToASingleHopChainOwnedByThisScope() {
            Scope.ResolvedRelation rel = assertInstanceOf(Scope.ResolvedRelation.class,
                    rootScope.resolve(TAGS));
            assertSame(rootScope, rel.owner());
            assertEquals(List.of("tags"),
                    rel.chain().stream().map(AttributeMapping.Relation::joinAttribute).toList());
        }

        @Test
        void dottedChainWalksEveryHop() {
            Scope.ResolvedRelation rel = assertInstanceOf(Scope.ResolvedRelation.class,
                    rootScope.resolve(CATEGORIES + ".subCategories"));
            assertSame(rootScope, rel.owner());
            assertEquals(List.of("categories", "subCategories"),
                    rel.chain().stream().map(AttributeMapping.Relation::joinAttribute).toList());
            assertEquals(SUB_CATEGORIES, rel.tail());
        }

        /**
         * {@code categories.name} resolves as a scalar with no path, so collection operators can
         * report it as a scalar. Asking for its column still fails.
         */
        @Test
        void fieldBehindARelationIsScalarButHasNoColumnHere() {
            Scope.ResolvedScalar scalar = assertInstanceOf(Scope.ResolvedScalar.class,
                    rootScope.resolve(CATEGORIES + ".name"));
            assertNull(scalar.path());
            assertEquals(AttributeMapping.field("name"), scalar.mapping());

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> rootScope.path(CATEGORIES + ".name"));
            assertTrue(ex.getMessage().contains("Unknown attribute"), ex.getMessage());
        }

        @Test
        void unmappedVariableThrows() {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> rootScope.resolve("request.resource.attr.nonexistent"));
            assertTrue(ex.getMessage().contains("Unknown attribute"), ex.getMessage());
        }

        @Test
        void relationIsRejectedWhereAColumnIsRequired() {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> rootScope.path(TAGS));
            assertTrue(ex.getMessage().contains("is a Relation"), ex.getMessage());
        }
    }

    @Nested
    class LambdaResolution {

        private Join<?, ?> categoryJoin;
        private Scope lambdaScope;

        @BeforeEach
        void enterLambda() {
            categoryJoin = root.join("categories");
            lambdaScope = categoriesLambda(rootScope, categoryJoin);
        }

        @Test
        void memberReferenceResolvesAgainstTheJoinedElement() {
            Scope.ResolvedScalar scalar = assertInstanceOf(Scope.ResolvedScalar.class,
                    lambdaScope.resolve("c.name"));
            assertSame(categoryJoin, scalar.path().getParentPath());
        }

        /** The bare lambda variable is the element: its mapping is the relation, not a Field. */
        @Test
        void bareLambdaVariableIsTheElementWithTheRelationAsItsMapping() {
            Join<?, ?> subCategoryJoin = categoryJoin.join("subCategories");
            Scope subLambda = Scope.lambda(subCategoryJoin, query, SUB_CATEGORIES, "s", lambdaScope);

            Scope.ResolvedScalar scalar = assertInstanceOf(Scope.ResolvedScalar.class,
                    subLambda.resolve("s"));
            // The path is the relation's defaultMemberField.
            assertSame(subCategoryJoin, scalar.path().getParentPath());
            assertSame(SUB_CATEGORIES, scalar.mapping());
        }

        @Test
        void memberRelationIsOwnedByTheLambdaScope() {
            Scope.ResolvedRelation rel = assertInstanceOf(Scope.ResolvedRelation.class,
                    lambdaScope.resolve("c.subCategories"));
            assertSame(lambdaScope, rel.owner());
            assertEquals(List.of("subCategories"),
                    rel.chain().stream().map(AttributeMapping.Relation::joinAttribute).toList());
        }

        /**
         * A root attribute referenced inside a lambda is owned by the root scope, not the lambda.
         * {@code chainSubquery} correlates {@code owner().from()}.
         */
        @Test
        void outerRelationIsOwnedByTheScopeThatHoldsIt() {
            Scope.ResolvedRelation rel = assertInstanceOf(Scope.ResolvedRelation.class,
                    lambdaScope.resolve(TAGS));
            assertSame(rootScope, rel.owner());
            assertSame(root, rel.owner().from());
        }

        /**
         * A sub-category also has a {@code categories} collection. Anchoring to the inner lambda
         * would still build a query, but over the wrong collection.
         */
        @Test
        void outerRelationIsNotCapturedByASameNamedCollectionOnTheElement() {
            Join<?, ?> subCategoryJoin = categoryJoin.join("subCategories");
            Scope subLambda = Scope.lambda(subCategoryJoin, query, SUB_CATEGORIES, "s", lambdaScope);

            Scope.ResolvedRelation rel = assertInstanceOf(Scope.ResolvedRelation.class,
                    subLambda.resolve(CATEGORIES));
            assertSame(rootScope, rel.owner());
            assertSame(root, rel.owner().from());
        }

        @Test
        void unprefixedVariableWithNoOuterScopeThrows() {
            Scope orphan = categoriesLambda(null, categoryJoin);
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> orphan.resolve(TAGS));
            assertTrue(ex.getMessage().contains("does not start with lambda variable"),
                    ex.getMessage());
        }
    }

    /** {@link Scope#rebaseAt} re-roots only the owning level; others keep their {@code From}. */
    @Nested
    class Rebasing {

        private Join<?, ?> categoryJoin;
        private Scope lambdaScope;
        private Subquery<Integer> sub;
        private From<?, ?> correlated;

        @BeforeEach
        void enterLambda() {
            categoryJoin = root.join("categories");
            lambdaScope = categoriesLambda(rootScope, categoryJoin);
            sub = query.subquery(Integer.class);
            correlated = sub.correlate(root);
        }

        @Test
        void rebasingTheRootScopeItselfReRootsItAtTheCorrelatedFrom() {
            Scope rebased = Scope.rebaseAt(rootScope, rootScope, correlated, sub);
            assertSame(correlated, rebased.from());
            assertSame(sub, rebased.parentQuery());
            assertSame(correlated,
                    rebased.path("request.resource.attr.aString").getParentPath());
        }

        /**
         * The lambda keeps its element join but uses the new subquery as the parent for deeper
         * subqueries.
         */
        @Test
        void rebasingAnOuterOwnerKeepsTheInterveningLambdaFrom() {
            Scope rebased = Scope.rebaseAt(lambdaScope, rootScope, correlated, sub);

            assertSame(categoryJoin, rebased.from());
            assertSame(sub, rebased.parentQuery());

            Path<?> outerPath = rebased.path("request.resource.attr.aString");
            assertSame(correlated, outerPath.getParentPath());

            Path<?> memberPath = rebased.path("c.name");
            assertSame(categoryJoin, memberPath.getParentPath());
        }

        @Test
        void rebasingTheLambdaLevelReRootsThatLevelOnly() {
            From<?, ?> correlatedJoin = sub.correlate(categoryJoin);
            Scope rebased = Scope.rebaseAt(lambdaScope, lambdaScope, correlatedJoin, sub);

            assertSame(correlatedJoin, rebased.from());
            assertSame(correlatedJoin, rebased.path("c.name").getParentPath());
            // The outer level is unchanged.
            assertSame(root, rebased.path("request.resource.attr.aString").getParentPath());
        }

        @Test
        void rebasingAtAScopeOffTheChainThrows() {
            Scope unrelated = Scope.root(query.from(ResourceEntity.class), query, MAPPER);
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> Scope.rebaseAt(lambdaScope, unrelated, correlated, sub));
            assertTrue(ex.getMessage().contains("not on the current resolution chain"),
                    ex.getMessage());
        }
    }
}
