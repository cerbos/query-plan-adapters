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
public class WorkspaceService {

    private static final Map<String, AttributeMapping> WORKSPACE_ATTRS = Map.of(
            "request.resource.attr.tenantId", AttributeMapping.field("tenantId"),
            "request.resource.attr.ownerId", AttributeMapping.field("ownerId"),
            "request.resource.attr.active", AttributeMapping.field("active"),
            "request.resource.attr.members", AttributeMapping.relation("members")
    );

    private final CerbosBlockingClient cerbos;
    private final WorkspaceRepository repository;

    public WorkspaceService(CerbosBlockingClient cerbos, WorkspaceRepository repository) {
        this.cerbos = cerbos;
        this.repository = repository;
    }

    public List<Workspace> listAllowed(AccessContext context, String action) {
        PlanResourcesResult plan = cerbos.plan(
                context.toPrincipal(), Resource.newInstance("workspace"), action);
        Result<Workspace> result =
                SpringDataQueryPlanAdapter.toSpecification(plan, WORKSPACE_ATTRS);
        Specification<Workspace> tenantBoundary = (root, query, cb) ->
                cb.equal(root.get("tenantId"), context.tenantId());
        return repository.findAll(tenantBoundary.and(result.toSpecification()));
    }
}
