package dev.cerbos.example.photos;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/photos")
public class PhotoController {

    public record LabelView(String name, double confidence, boolean reviewed) {
        static LabelView from(PhotoLabel label) {
            return new LabelView(label.getLabelName(), label.getConfidence(), label.isReviewed());
        }
    }

    public record PhotoView(String id, String tenantId, String ownerId, String title, boolean isPublic,
                            boolean isArchived, String location, int rating, int width, int height,
                            Set<String> tags, Set<LabelView> labels) {
        static PhotoView from(Photo p) {
            return new PhotoView(p.getId(), p.getTenantId(), p.getOwnerId(), p.getTitle(), p.isPublic(),
                    p.isArchived(), p.getLocation(), p.getRating(), p.getDetails().getPixelWidth(),
                    p.getDetails().getPixelHeight(), p.getTags(), p.getLabels().stream()
                    .map(LabelView::from)
                    .collect(Collectors.toCollection(LinkedHashSet::new)));
        }
    }

    private final PhotoService service;

    public PhotoController(PhotoService service) {
        this.service = service;
    }

    /**
     * GET /photos?user=alice&role=user&action=view
     *
     * <p><strong>DEMO ONLY — never copy this identity handling.</strong> Identity, role,
     * tenant, and groups arrive as unauthenticated query parameters purely so the curl/smoke
     * harness can switch principals per request. In production, derive the principal from
     * your authentication layer — Spring Security's {@code Authentication} /
     * {@code SecurityContextHolder}, a verified JWT/OIDC token, or equivalent — never from
     * request input (parameters, headers, or bodies). As written, any caller can assert
     * {@code role=admin} (hitting the unconditional-ALLOW rule in {@code photo.yaml}) or
     * pick another tenant; an authorization filter built from a caller-asserted principal
     * authorizes nothing. See the "Demo identity only" warning in the example README.
     */
    @GetMapping
    public List<PhotoView> list(@RequestParam String user,
                                @RequestParam(defaultValue = "user") String role,
                                @RequestParam(defaultValue = "view") String action,
                                @RequestParam(defaultValue = "acme") String tenant,
                                @RequestParam(defaultValue = "") String groups,
                                @RequestParam(defaultValue = "") String interests,
                                @RequestParam(required = false) Integer minRating) {
        validateMinRating(minRating);
        return service.listAllowed(context(user, role, tenant, groups, interests), action, minRating)
                .stream()
                .map(PhotoView::from)
                .toList();
    }

    /**
     * GET /photos/page?user=alice&action=needs-moderation&page=0&size=1
     *
     * <p><strong>DEMO ONLY:</strong> same caveat as {@link #list} — the principal comes from
     * query parameters for harness reproducibility; production code must derive it from the
     * authentication layer, never from request input.
     */
    @GetMapping("/page")
    public Page<PhotoView> page(@RequestParam String user,
                                @RequestParam(defaultValue = "user") String role,
                                @RequestParam(defaultValue = "view") String action,
                                @RequestParam(defaultValue = "acme") String tenant,
                                @RequestParam(defaultValue = "") String groups,
                                @RequestParam(defaultValue = "") String interests,
                                @RequestParam(required = false) Integer minRating,
                                @RequestParam(defaultValue = "0") int page,
                                @RequestParam(defaultValue = "2") int size) {
        validateMinRating(minRating);
        PageRequest pageRequest = pageRequest(page, size);
        return service.listAllowed(
                        context(user, role, tenant, groups, interests), action, minRating, pageRequest)
                .map(PhotoView::from);
    }

    /**
     * DELETE /photos/bulk-unsafe?user=alice&action=comment
     *
     * <p><strong>Intentionally-failing demonstration of the adapter's bulk-delete guard —
     * this endpoint is expected to return 409, not delete anything.</strong> It passes the
     * Cerbos Specification to {@code JpaSpecificationExecutor.delete(Specification)}, which
     * the adapter forbids whenever the plan touches a Relation mapping: Hibernate's
     * multi-table bulk delete would first clear the collection tables the correlated EXISTS
     * subquery references, silently destroying tag/label/grant rows while deleting zero
     * photos (PR #273). The adapter detects the bulk-delete invocation context and throws
     * {@link UnsupportedOperationException} before any SQL runs; this endpoint surfaces that
     * as HTTP 409 with the guard's message. The smoke harness asserts the 409 and then
     * proves the photo and collection rows are untouched. The correct deletion pattern is
     * {@code findAll(spec)} then {@code deleteAllById(ids)}.
     */
    @DeleteMapping("/bulk-unsafe")
    public ResponseEntity<String> bulkUnsafeDelete(@RequestParam String user,
                                                   @RequestParam(defaultValue = "user") String role,
                                                   @RequestParam(defaultValue = "comment") String action,
                                                   @RequestParam(defaultValue = "acme") String tenant,
                                                   @RequestParam(defaultValue = "") String groups,
                                                   @RequestParam(defaultValue = "") String interests) {
        try {
            long deleted = service.unsafeBulkDelete(
                    context(user, role, tenant, groups, interests), action);
            // The guard failing to fire is itself a regression — make it loud.
            return ResponseEntity.internalServerError()
                    .body("bulk-delete guard did not fire; rows deleted: " + deleted);
        } catch (UnsupportedOperationException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage());
        }
    }

    private static AccessContext context(String user, String role, String tenant,
                                         String groups, String interests) {
        return new AccessContext(
                user, role, tenant, parseCsv(groups), parseCsv(interests));
    }

    private static void validateMinRating(Integer minRating) {
        if (minRating != null && (minRating < 0 || minRating > 5)) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "minRating must be between 0 and 5");
        }
    }

    private static PageRequest pageRequest(int page, int size) {
        if (page < 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "page must be at least 0");
        }
        if (size < 1 || size > 100) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "size must be between 1 and 100");
        }
        return PageRequest.of(page, size, Sort.by("id").ascending());
    }

    private static Set<String> parseCsv(String csv) {
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }
}
