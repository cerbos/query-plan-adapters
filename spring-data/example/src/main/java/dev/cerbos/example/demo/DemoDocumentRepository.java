package dev.cerbos.example.demo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

/**
 * {@code JpaSpecificationExecutor} is the whole point: it is the real Spring Data query method
 * the adapter's {@code Specification} has to be accepted by, in both its unpaged
 * ({@code findAll(Specification)}) and paged ({@code findAll(Specification, Pageable)}) forms.
 * The paged form fires a second COUNT query with its own {@code CriteriaQuery} and {@code Root},
 * which is a shape no conformance harness exercises.
 */
public interface DemoDocumentRepository
        extends JpaRepository<DemoDocument, String>, JpaSpecificationExecutor<DemoDocument> {
}
