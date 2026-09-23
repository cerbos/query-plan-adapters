/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.example.photos;

import dev.cerbos.queryplan.springdata.AttributeMapping;
import dev.cerbos.queryplan.springdata.SpringDataQueryPlanAdapter;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.PlanResourcesResult;
import dev.cerbos.sdk.builders.Resource;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class PhotoService {

    // Maps policy attribute paths to JPA paths on Photo. Relation mappings make collection
    // operators emit correlated subqueries, and their nested maps rename child fields.
    private static final Map<String, AttributeMapping> PHOTO_ATTRS = Map.ofEntries(
            Map.entry("request.resource.attr.ownerId", AttributeMapping.field("ownerId")),
            Map.entry("request.resource.attr.tenantId", AttributeMapping.field("tenantId")),
            Map.entry("request.resource.attr.public", AttributeMapping.field("isPublic")),
            Map.entry("request.resource.attr.archived", AttributeMapping.field("isArchived")),
            Map.entry("request.resource.attr.title", AttributeMapping.field("title")),
            Map.entry("request.resource.attr.location", AttributeMapping.field("location")),
            Map.entry("request.resource.attr.rating", AttributeMapping.field("rating")),
            // Used only by the edge-* scenarios in scripts/smoke-edge-cases.sh.
            Map.entry("request.resource.attr.score", AttributeMapping.field("score")),
            Map.entry("request.resource.attr.createdAt", AttributeMapping.field("createdAt")),
            Map.entry("request.resource.attr.metadata.width", AttributeMapping.field("details.pixelWidth")),
            Map.entry("request.resource.attr.tags", AttributeMapping.relation("tags")),
            Map.entry("request.resource.attr.labels", AttributeMapping.relation("labels", Map.of(
                    "name", AttributeMapping.field("labelName"),
                    "confidence", AttributeMapping.field("confidence"),
                    "reviewed", AttributeMapping.field("reviewed")
            ))),
            Map.entry("request.resource.attr.grants", AttributeMapping.relation("grants", Map.of(
                    "tenantId", AttributeMapping.field("tenantId"),
                    "permission", AttributeMapping.field("permission"),
                    "userId", AttributeMapping.field("userId"),
                    "groupId", AttributeMapping.field("groupId")
            )))
    );

    private final CerbosBlockingClient cerbos;
    private final PhotoRepository repository;

    public PhotoService(CerbosBlockingClient cerbos, PhotoRepository repository) {
        this.cerbos = cerbos;
        this.repository = repository;
    }

    public List<Photo> listAllowed(AccessContext context, String action, Integer minRating) {
        return repository.findAll(specification(context, action, minRating));
    }

    public Page<Photo> listAllowed(AccessContext context, String action, Integer minRating,
                                   Pageable pageable) {
        return repository.findAll(specification(context, action, minRating), pageable);
    }

    /**
     * Passes the Cerbos Specification to {@code delete(Specification)}, which the adapter
     * refuses. Exists only for {@code DELETE /photos/bulk-unsafe}.
     *
     * <p>When the plan uses a relation mapping, the adapter throws
     * {@link UnsupportedOperationException}: Hibernate's bulk delete would clear the collection
     * tables first and delete no photos. Use {@code findAll(spec)} and {@code deleteAllById(ids)}
     * instead.
     */
    public long unsafeBulkDelete(AccessContext context, String action) {
        return repository.delete(specification(context, action, null));
    }

    private Specification<Photo> specification(AccessContext context, String action,
                                                Integer minRating) {
        PlanResourcesResult plan = cerbos.plan(
                context.toPrincipal(),
                Resource.newInstance("photo"),
                List.of(action));

        Specification<Photo> allowed = SpringDataQueryPlanAdapter.toSpecification(plan, PHOTO_ATTRS);
        Specification<Photo> tenantBoundary = (root, query, cb) ->
                cb.equal(root.get("tenantId"), context.tenantId());
        Specification<Photo> specification = tenantBoundary.and(allowed);
        if (minRating != null) {
            specification = specification.and((root, query, cb) ->
                    cb.greaterThanOrEqualTo(root.get("rating"), minRating));
        }
        return specification;
    }
}
