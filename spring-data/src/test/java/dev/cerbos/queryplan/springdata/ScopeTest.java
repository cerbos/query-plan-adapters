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
 * Tests for the variable-resolution seam itself, rather than through a translated query.
 *
 * <p>{@link Scope#resolve} is the adapter's one classifier: it decides whether a plan variable
 * denotes a column or a collection, and — for a collection — which scope OWNS it. That
 * ownership is what {@link Scope#rebaseAt} and {@code chainSubquery} anchor a correlated
 * subquery to, and getting it wrong is silent: the element entity of an enclosing lambda can
 * carry a collection of the same name ({@link dev.cerbos.queryplan.springdata.testmodel.SubCategoryEntity}
 * has its own {@code categories}), so a subquery anchored to the lambda join still builds and
 * still returns rows — the wrong ones. Proving it here rather than only through multi-hop
 * collection queries means the invariant is stated where it lives.
 */
class ScopeTest {

    private static final String TAGS = "request.resource.attr.tags";
    private static final String CATEGORIES = "request.resource.attr.categories";

    private static final AttributeMapping.Relation SUB_CATEGORIES =
            AttributeMapping.relation("subCategories", "name",
                    Map.of("name", AttributeMapping.field("name")));

    private static final Map<String, AttributeMapping> MAPPER = Map.of(
            "request.resource.attr.aString", AttributeMapping.field("aString"),
            "request.resource.attr.aOptionalString",
            AttributeMapping.field("aOptionalString", NullAttributeRepresentation.EXPLICIT),
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

    /** A lambda scope over {@code categories}, as {@code categories.exists(c, ...)} builds. */
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

        /**
         * The declared NULL convention travels on the resolution, which is what
         * {@code isExplicitNull} reads — the JPA path cannot discriminate two attributes
         * mapped to the same column under different conventions.
         */
        @Test
        void fieldCarriesItsDeclaredNullConvention() {
            Scope.ResolvedScalar scalar = assertInstanceOf(Scope.ResolvedScalar.class,
                    rootScope.resolve("request.resource.attr.aOptionalString"));
            assertEquals(NullAttributeRepresentation.EXPLICIT,
                    ((AttributeMapping.Field) scalar.mapping()).nullAttributeRepresentation());
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
         * A Field reached THROUGH a relation ({@code categories.name}) is a scalar per
         * ELEMENT: mapped — so the collection operators can say "that is a scalar" rather than
         * "unknown attribute" — but with no column on the root entity, so asking for its path
         * is the "Unknown attribute" it has always been.
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

        /**
         * The bare lambda variable denotes the ELEMENT, not a relation — it has a path (the
         * relation's scalar projection, or the join itself) but no Field mapping of its own,
         * which is how {@code size(t)} stays rejected while {@code size(aString)} does not.
         */
        @Test
        void bareLambdaVariableIsTheElementWithTheRelationAsItsMapping() {
            Join<?, ?> subCategoryJoin = categoryJoin.join("subCategories");
            Scope subLambda = Scope.lambda(subCategoryJoin, query, SUB_CATEGORIES, "s", lambdaScope);

            Scope.ResolvedScalar scalar = assertInstanceOf(Scope.ResolvedScalar.class,
                    subLambda.resolve("s"));
            // The relation's defaultMemberField stands in for the bare element.
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
         * OWNER ANCHORING. {@code request.resource.attr.tags} referenced inside a
         * {@code categories.exists(c, ...)} body resolves outward, and the owner it comes back
         * with must be the scope that HOLDS the attribute — the root — not the scope that
         * happened to resolve it. {@code chainSubquery} correlates {@code owner().from()}, so
         * an owner of {@code lambdaScope} would join {@code tags} off the category join.
         */
        @Test
        void outerRelationIsOwnedByTheScopeThatHoldsIt() {
            Scope.ResolvedRelation rel = assertInstanceOf(Scope.ResolvedRelation.class,
                    lambdaScope.resolve(TAGS));
            assertSame(rootScope, rel.owner());
            assertSame(root, rel.owner().from());
        }

        /**
         * The same anchoring where getting it wrong is SILENT rather than a build error: a
         * sub-category element carries its own {@code categories} collection, so anchoring
         * {@code request.resource.attr.categories} to the innermost lambda would still build
         * and still return rows — a different entity's same-named collection.
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

    /**
     * {@link Scope#rebaseAt} re-roots exactly the level that owns the relation, so paths
     * resolved through it become correlation references of the new subquery while every other
     * level keeps its own {@code From}.
     */
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
         * Rebasing an outer-owned relation from inside a lambda touches ONLY the root level:
         * the lambda keeps its element join (its member references stay legal), but adopts the
         * subquery as the query any deeper subqueries are built against.
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
            // The outer level is untouched — it stays a legal implicit correlation reference.
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
