package dev.cerbos.example.photos;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Set;

@RestController
@RequestMapping("/albums")
public class AlbumController {

    public record AlbumView(String id, String tenantId, String ownerId, String title,
                            boolean shared, Set<String> collaborators) {
        static AlbumView from(Album album) {
            return new AlbumView(album.getId(), album.getTenantId(), album.getOwnerId(),
                    album.getTitle(), album.isShared(), album.getCollaborators());
        }
    }

    private final AlbumService service;

    public AlbumController(AlbumService service) {
        this.service = service;
    }

    @GetMapping
    public List<AlbumView> list(@RequestParam String user,
                                @RequestParam(defaultValue = "user") String role,
                                @RequestParam(defaultValue = "acme") String tenant,
                                @RequestParam(defaultValue = "view") String action) {
        AccessContext context = new AccessContext(user, role, tenant, Set.of(), Set.of());
        return service.listAllowed(context, action).stream().map(AlbumView::from).toList();
    }
}
