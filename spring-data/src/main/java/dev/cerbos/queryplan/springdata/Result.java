package dev.cerbos.queryplan.springdata;

import org.springframework.data.jpa.domain.Specification;

public sealed interface Result<T> permits Result.AlwaysAllowed, Result.AlwaysDenied, Result.Conditional {

    /**
     * Returns a {@link Specification} that captures this result, so it composes cleanly with the
     * caller's own Specifications via {@code .and(...)} / {@code .or(...)}:
     *
     * <ul>
     *   <li>{@link AlwaysAllowed} – {@code null} predicate; Spring Data's
     *       {@code SimpleJpaRepository} treats this as "no restriction" and omits the
     *       {@code WHERE} clause entirely (matches {@link Specification#unrestricted()}).</li>
     *   <li>{@link AlwaysDenied} – always-false predicate ({@code 1=0}).</li>
     *   <li>{@link Conditional} – the wrapped Specification. The lambda is invoked fresh
     *       for every query (including Spring Data's separate COUNT pass under
     *       {@code findAll(spec, Pageable)}), so callers must not cache the produced
     *       {@code Predicate} across query executions.</li>
     * </ul>
     */
    Specification<T> toSpecification();

    record AlwaysAllowed<T>() implements Result<T> {
        @Override
        public Specification<T> toSpecification() {
            // Returning null is the canonical "no restriction" signal — Spring Data's
            // SimpleJpaRepository.applySpecificationToCriteria guards with
            // `if (predicate != null) query.where(predicate)`, so this avoids emitting
            // `WHERE 1=1` and keeps composition with `.and(otherSpec)` clean.
            return (root, query, cb) -> null;
        }
    }

    record AlwaysDenied<T>() implements Result<T> {
        @Override
        public Specification<T> toSpecification() {
            return (root, query, cb) -> cb.disjunction();
        }
    }

    /**
     * Wraps the translated Specification. The contained Specification is a fresh lambda that
     * rebuilds the entire predicate tree from the {@code Root}/{@code CriteriaQuery} passed in
     * on each invocation — this is required because Spring Data's
     * {@code JpaSpecificationExecutor.findAll(spec, Pageable)} fires a separate {@code COUNT}
     * query with its own {@code CriteriaQuery} and {@code Root}, and Hibernate 6 rejects a
     * cached {@code Predicate} produced against a different {@code Root}
     * ({@code SqlTreeCreationException: Could not locate TableGroup}). Callers must therefore
     * never cache or re-use the {@code Predicate} returned by
     * {@link Specification#toPredicate}; pass the Specification itself to repository methods
     * and let Spring Data invoke it once per query.
     */
    record Conditional<T>(Specification<T> specification) implements Result<T> {
        @Override
        public Specification<T> toSpecification() {
            return specification;
        }
    }
}

