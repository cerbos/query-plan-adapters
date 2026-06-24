package dev.cerbos.example.photos;

import dev.cerbos.queryplan.springdata.AttributeMapping;
import dev.cerbos.queryplan.springdata.Result;
import dev.cerbos.queryplan.springdata.SpringDataQueryPlanAdapter;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.PlanResourcesResult;
import dev.cerbos.sdk.builders.Principal;
import dev.cerbos.sdk.builders.Resource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class PhotoService {

    /**
     * Maps Cerbos resource-attribute paths used in the policy to JPA paths on {@link Photo}.
     * The Spring Data adapter translates each plan operand to {@code root.get(...)} via this
     * mapping. {@code tags} is an {@code @ElementCollection<String>} — declaring it as a
     * relation makes the adapter emit a correlated {@code EXISTS} subquery for the CEL
     * {@code "x" in tags} predicate.
     */
    private static final Map<String, AttributeMapping> PHOTO_ATTRS = Map.of(
            "request.resource.attr.ownerId", AttributeMapping.field("ownerId"),
            "request.resource.attr.public", AttributeMapping.field("isPublic"),
            "request.resource.attr.archived", AttributeMapping.field("isArchived"),
            "request.resource.attr.tags", AttributeMapping.relation("tags")
    );

    private final CerbosBlockingClient cerbos;
    private final PhotoRepository repository;

    public PhotoService(CerbosBlockingClient cerbos, PhotoRepository repository) {
        this.cerbos = cerbos;
        this.repository = repository;
    }

    public List<Photo> listAllowed(String userId, String role, String action) {
        PlanResourcesResult plan = cerbos.plan(
                Principal.newInstance(userId).withRoles(role),
                Resource.newInstance("photo"),
                action);

        Result<Photo> result = SpringDataQueryPlanAdapter.toSpecification(plan, PHOTO_ATTRS);
        return repository.findAll(result.toSpecification());
    }
}
