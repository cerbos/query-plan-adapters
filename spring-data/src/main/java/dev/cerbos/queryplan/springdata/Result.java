package dev.cerbos.queryplan.springdata;

import org.springframework.data.jpa.domain.Specification;

public sealed interface Result<T> permits Result.AlwaysAllowed, Result.AlwaysDenied, Result.Conditional {

    /**
     * Returns a {@link Specification} that captures this result, so it composes cleanly with the
     * caller's own Specifications via {@code .and(...)} / {@code .or(...)}:
     *
     * <ul>
     *   <li>{@link AlwaysAllowed} – always-true predicate ({@code 1=1})</li>
     *   <li>{@link AlwaysDenied} – always-false predicate ({@code 1=0})</li>
     *   <li>{@link Conditional} – the wrapped Specification</li>
     * </ul>
     */
    Specification<T> toSpecification();

    record AlwaysAllowed<T>() implements Result<T> {
        @Override
        public Specification<T> toSpecification() {
            return (root, query, cb) -> cb.conjunction();
        }
    }

    record AlwaysDenied<T>() implements Result<T> {
        @Override
        public Specification<T> toSpecification() {
            return (root, query, cb) -> cb.disjunction();
        }
    }

    record Conditional<T>(Specification<T> specification) implements Result<T> {
        @Override
        public Specification<T> toSpecification() {
            return specification;
        }
    }
}

