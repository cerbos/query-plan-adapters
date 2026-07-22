package dev.cerbos.example.photos;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Set;

@RestController
@RequestMapping("/workspaces")
public class WorkspaceController {

    public record WorkspaceView(String id, String tenantId, String ownerId, String name,
                                boolean active, Set<String> members) {
        static WorkspaceView from(Workspace workspace) {
            return new WorkspaceView(workspace.getId(), workspace.getTenantId(),
                    workspace.getOwnerId(), workspace.getName(), workspace.isActive(),
                    workspace.getMembers());
        }
    }

    private final WorkspaceService service;

    public WorkspaceController(WorkspaceService service) {
        this.service = service;
    }

    @GetMapping
    public List<WorkspaceView> list(@RequestParam String user,
                                    @RequestParam(defaultValue = "user") String role,
                                    @RequestParam(defaultValue = "acme") String tenant,
                                    @RequestParam(defaultValue = "access") String action) {
        AccessContext context = new AccessContext(user, role, tenant, Set.of(), Set.of());
        return service.listAllowed(context, action).stream().map(WorkspaceView::from).toList();
    }
}
