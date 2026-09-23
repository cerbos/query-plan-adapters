/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.springdata.testmodel.CategoryEntity;
import dev.cerbos.queryplan.springdata.testmodel.LabelEntity;
import dev.cerbos.queryplan.springdata.testmodel.ResourceEntity;
import dev.cerbos.queryplan.springdata.testmodel.SubCategoryEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import jakarta.persistence.Persistence;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.jpa.domain.Specification;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Bounds the SQL size of nested {@code exists} chains. The lambda body is translated once per
 * polarity (see {@link TriPredicate}), so the subquery count grows with depth. The test asserts at
 * most {@code 2^d - 1} correlated subqueries for depth 1 to 4 and prints timings without asserting
 * them. Runs offline on H2.
 *
 * <p>Plans are hand-built because the corpus stops at depth 3.
 */
class MacroNestingBenchmarkTest {

    /** Captures the SQL of every statement executed through this suite's EMF. */
    public static final class BenchSqlCapture
            implements org.hibernate.resource.jdbc.spi.StatementInspector {
        static final List<String> STATEMENTS =
                java.util.Collections.synchronizedList(new ArrayList<>());

        @Override
        public String inspect(String sql) {
            STATEMENTS.add(sql);
            return sql;
        }
    }

    private static final int RESOURCES = 300;
    private static final int CATEGORIES = 15;
    private static final int SUBCATEGORIES = 45;
    private static final int LABELS = 135;

    private static final Map<String, AttributeMapping> MAPPING = Map.of(
            "request.resource.attr.categories", AttributeMapping.relation("categories", Map.of(
                    "name", AttributeMapping.field("name"),
                    "subCategories", AttributeMapping.relation("subCategories", Map.of(
                            "name", AttributeMapping.field("name"),
                            "labels", AttributeMapping.relation("labels", Map.of(
                                    "name", AttributeMapping.field("name"),
                                    // labels and subCategories map each other, giving a fourth hop.
                                    "subCategories", AttributeMapping.relation("subCategories", Map.of(
                                            "name", AttributeMapping.field("name")
                                    ))
                            ))
                    ))
            ))
    );

    private static EntityManagerFactory emf;

    @BeforeAll
    static void setUp() {
        // A separate database, so these rows stay out of other suites using test-pu.
        emf = Persistence.createEntityManagerFactory("test-pu", Map.of(
                "jakarta.persistence.jdbc.url",
                "jdbc:h2:mem:cerbosbench;DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
                "hibernate.session_factory.statement_inspector",
                BenchSqlCapture.class.getName()));
        seed();
    }

    @AfterAll
    static void tearDown() {
        if (emf != null) emf.close();
    }

    /** 300 resources with 3 categories each, 3 subCategories per category, 3 labels per sub. */
    private static void seed() {
        EntityManager em = emf.createEntityManager();
        EntityTransaction tx = em.getTransaction();
        tx.begin();

        List<LabelEntity> labels = new ArrayList<>();
        for (int i = 0; i < LABELS; i++) {
            LabelEntity l = new LabelEntity("lab-" + i, "lab-" + (i % 9));
            em.persist(l);
            labels.add(l);
        }
        List<SubCategoryEntity> subs = new ArrayList<>();
        for (int i = 0; i < SUBCATEGORIES; i++) {
            SubCategoryEntity s = new SubCategoryEntity("sub-" + i, "sub-" + (i % 5));
            s.setLabels(new ArrayList<>(List.of(
                    labels.get((i * 3) % LABELS),
                    labels.get((i * 3 + 1) % LABELS),
                    labels.get((i * 3 + 2) % LABELS))));
            em.persist(s);
            subs.add(s);
        }
        List<CategoryEntity> cats = new ArrayList<>();
        for (int i = 0; i < CATEGORIES; i++) {
            CategoryEntity c = new CategoryEntity("cat-" + i, "cat-" + (i % 4));
            c.setSubCategories(new ArrayList<>(List.of(
                    subs.get((i * 3) % SUBCATEGORIES),
                    subs.get((i * 3 + 1) % SUBCATEGORIES),
                    subs.get((i * 3 + 2) % SUBCATEGORIES))));
            em.persist(c);
            cats.add(c);
        }
        for (int i = 0; i < RESOURCES; i++) {
            ResourceEntity r = new ResourceEntity("res-" + i);
            r.setCategories(new ArrayList<>(List.of(
                    cats.get(i % CATEGORIES),
                    cats.get((i + 5) % CATEGORIES),
                    cats.get((i + 10) % CATEGORIES))));
            em.persist(r);
            if (i % 100 == 99) {
                em.flush();
                em.clear();
            }
        }
        tx.commit();
        em.close();
    }

    // -- plan-operand builders --

    private static Operand exprOp(String op, Operand... operands) {
        Expression.Builder e = Expression.newBuilder().setOperator(op);
        for (Operand o : operands) e.addOperands(o);
        return Operand.newBuilder().setExpression(e).build();
    }

    private static Operand var(String name) {
        return Operand.newBuilder().setVariable(name).build();
    }

    private static Operand sval(String v) {
        return Operand.newBuilder().setValue(Value.newBuilder().setStringValue(v)).build();
    }

    private static Operand lambda(String varName, Operand body) {
        return exprOp("lambda", body, var(varName));
    }

    /** The relation each level iterates, and the name the innermost level matches. */
    private static final String[] HOPS = {"subCategories", "labels", "subCategories"};
    private static final String[] LEAF = {"cat-1", "sub-1", "lab-1", "sub-1"};

    /** {@code categories.exists(v1, v1.subCategories.exists(v2, ... vd.name == LEAF))}. */
    private static Operand existsChain(int depth) {
        return existsLevel(1, depth, "request.resource.attr.categories");
    }

    private static Operand existsLevel(int level, int depth, String collection) {
        String v = "v" + level;
        Operand body = level == depth
                ? exprOp("eq", var(v + ".name"), sval(LEAF[depth - 1]))
                : existsLevel(level + 1, depth, v + "." + HOPS[level - 1]);
        return exprOp("exists", var(collection), lambda(v, body));
    }

    private record Measurement(int depth, int subqueries, long translateMicros,
                               long executeMicros, int rows) {}

    private Measurement measure(int depth) {
        PlanResourcesResponse resp = PlanResourcesResponse.newBuilder()
                .setFilter(PlanResourcesFilter.newBuilder()
                        .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                        .setCondition(existsChain(depth)))
                .build();
        Specification<ResourceEntity> spec =
                SpringDataQueryPlanAdapter.toSpecification(resp, MAPPING);

        EntityManager em = emf.createEntityManager();
        try {
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<String> cq = cb.createQuery(String.class);
            Root<ResourceEntity> root = cq.from(ResourceEntity.class);
            cq.select(root.get("id")).distinct(true);

            long t0 = System.nanoTime();
            Predicate p = spec.toPredicate(root, cq, cb);
            long translateNanos = System.nanoTime() - t0;
            cq.where(p);

            BenchSqlCapture.STATEMENTS.clear();
            long t1 = System.nanoTime();
            List<String> rows = em.createQuery(cq).getResultList();
            long executeNanos = System.nanoTime() - t1;

            String sql = BenchSqlCapture.STATEMENTS.stream()
                    .filter(s -> s.toLowerCase(Locale.ROOT).startsWith("select"))
                    .reduce("", (a, b) -> a.length() >= b.length() ? a : b)
                    .toLowerCase(Locale.ROOT);
            int subqueries = countOccurrences(sql, "select") - 1;
            return new Measurement(depth, subqueries,
                    translateNanos / 1_000, executeNanos / 1_000, rows.size());
        } finally {
            em.close();
        }
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + 1)) {
            count++;
        }
        return count;
    }

    @Test
    void nestedExistsSubqueryCountStaysSingleSubqueryPerPolarity() {
        List<Measurement> results = new ArrayList<>();
        for (int depth = 1; depth <= 4; depth++) {
            // Warm up once so first-use initialization does not skew the timings.
            measure(depth);
            results.add(measure(depth));
        }

        System.out.println("== nested exists-chain translation cost (H2, "
                + RESOURCES + " resources, 3x3x3 shared relation pool) ==");
        System.out.printf("%-6s %-22s %-18s %-16s %s%n",
                "depth", "correlated subqueries", "translate (us)", "execute (us)", "rows");
        for (Measurement m : results) {
            System.out.printf("%-6d %-22d %-18d %-16d %d%n",
                    m.depth(), m.subqueries(), m.translateMicros(), m.executeMicros(), m.rows());
        }

        for (Measurement m : results) {
            int bound = (1 << m.depth()) - 1; // 2^d - 1
            assertTrue(m.subqueries() <= bound,
                    "depth-" + m.depth() + " exists chain emitted " + m.subqueries()
                            + " correlated subqueries (bound " + bound + ") — the macro "
                            + "translation has regressed toward the multi-probe shape");
        }
    }
}
