package dev.cerbos.example.photos;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Set;

@RestController
@RequestMapping("/photos")
public class PhotoController {

    public record PhotoView(String id, String ownerId, String title, boolean isPublic,
                            boolean isArchived, String location, Set<String> tags) {
        static PhotoView from(Photo p) {
            return new PhotoView(p.getId(), p.getOwnerId(), p.getTitle(), p.isPublic(),
                    p.isArchived(), p.getLocation(), p.getTags());
        }
    }

    private final PhotoService service;

    public PhotoController(PhotoService service) {
        this.service = service;
    }

    /** GET /photos?user=alice&role=user&action=view */
    @GetMapping
    public List<PhotoView> list(@RequestParam String user,
                                @RequestParam(defaultValue = "user") String role,
                                @RequestParam(defaultValue = "view") String action) {
        return service.listAllowed(user, role, action).stream().map(PhotoView::from).toList();
    }
}
