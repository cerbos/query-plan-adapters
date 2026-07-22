package dev.cerbos.example.photos;

import dev.cerbos.queryplan.springdata.AttributeMapping;
import dev.cerbos.queryplan.springdata.Result;
import dev.cerbos.queryplan.springdata.SpringDataQueryPlanAdapter;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.PlanResourcesResult;
import dev.cerbos.sdk.builders.Resource;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class AlbumService {

    private static final Map<String, AttributeMapping> ALBUM_ATTRS = Map.of(
            "request.resource.attr.tenantId", AttributeMapping.field("tenantId"),
            "request.resource.attr.ownerId", AttributeMapping.field("ownerId"),
            "request.resource.attr.shared", AttributeMapping.field("shared"),
            "request.resource.attr.collaborators", AttributeMapping.relation("collaborators")
    );

    private final CerbosBlockingClient cerbos;
    private final AlbumRepository repository;

    public AlbumService(CerbosBlockingClient cerbos, AlbumRepository repository) {
        this.cerbos = cerbos;
        this.repository = repository;
    }

    public List<Album> listAllowed(AccessContext context, String action) {
        PlanResourcesResult plan = cerbos.plan(
                context.toPrincipal(), Resource.newInstance("album"), action);
        Result<Album> result = SpringDataQueryPlanAdapter.toSpecification(plan, ALBUM_ATTRS);
        Specification<Album> tenantBoundary = (root, query, cb) ->
                cb.equal(root.get("tenantId"), context.tenantId());
        return repository.findAll(tenantBoundary.and(result.toSpecification()));
    }
}
