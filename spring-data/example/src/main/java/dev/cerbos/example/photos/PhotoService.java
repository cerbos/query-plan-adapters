package dev.cerbos.example.photos;

import dev.cerbos.queryplan.springdata.AttributeMapping;
import dev.cerbos.queryplan.springdata.Result;
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

    /**
     * Maps Cerbos resource-attribute paths used in the policy to JPA paths on {@link Photo}.
     * The Spring Data adapter translates each plan operand to {@code root.get(...)} via this
     * mapping. Declaring {@code tags} and {@code labels} as relations makes collection operators
     * emit correlated subqueries; the nested labels map also decouples policy field names from
     * Java property names.
     */
    private static final Map<String, AttributeMapping> PHOTO_ATTRS = Map.ofEntries(
            Map.entry("request.resource.attr.ownerId", AttributeMapping.field("ownerId")),
            Map.entry("request.resource.attr.tenantId", AttributeMapping.field("tenantId")),
            Map.entry("request.resource.attr.public", AttributeMapping.field("isPublic")),
            Map.entry("request.resource.attr.archived", AttributeMapping.field("isArchived")),
            Map.entry("request.resource.attr.title", AttributeMapping.field("title")),
            Map.entry("request.resource.attr.location", AttributeMapping.field("location")),
            Map.entry("request.resource.attr.rating", AttributeMapping.field("rating")),
            // Edge-case regression scenarios (scripts/smoke-edge-cases.sh): a nullable
            // double for the IEEE arithmetic probes and an Instant column for the
            // timestamp()/retention-window rule.
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
     * Deliberately passes the Cerbos Specification to
     * {@code JpaSpecificationExecutor.delete(Specification)} — the one repository operation
     * the adapter forbids. Whenever the plan touches a Relation mapping (tags, labels,
     * grants), the adapter throws {@link UnsupportedOperationException} instead of letting
     * Hibernate's multi-table bulk delete corrupt the collection tables (PR #273: the bulk
     * delete first clears the @ElementCollection/join rows the correlated EXISTS references,
     * so 0 entity rows were deleted while their collection rows were silently destroyed).
     * Exists only so {@code DELETE /photos/bulk-unsafe} can demonstrate — and the smoke
     * harness can pin — that guard. Production code should select ids with
     * {@code findAll(spec)} and delete via {@code deleteAllById(ids)}.
     */
    public long unsafeBulkDelete(AccessContext context, String action) {
        return repository.delete(specification(context, action, null));
    }

    private Specification<Photo> specification(AccessContext context, String action,
                                                Integer minRating) {
        PlanResourcesResult plan = cerbos.plan(
                context.toPrincipal(),
                Resource.newInstance("photo"),
                action);

        Result<Photo> result = SpringDataQueryPlanAdapter.toSpecification(plan, PHOTO_ATTRS);
        Specification<Photo> tenantBoundary = (root, query, cb) ->
                cb.equal(root.get("tenantId"), context.tenantId());
        Specification<Photo> specification = tenantBoundary.and(result.toSpecification());
        if (minRating != null) {
            specification = specification.and((root, query, cb) ->
                    cb.greaterThanOrEqualTo(root.get("rating"), minRating));
        }
        return specification;
    }
}
