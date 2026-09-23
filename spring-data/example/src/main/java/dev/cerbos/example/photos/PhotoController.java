/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

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
     * Lists the photos the principal may perform {@code action} on.
     *
     * <p><strong>Demo only: do not copy this identity handling.</strong> The principal comes from
     * unauthenticated query parameters so the smoke scripts can switch users. Any caller can
     * claim any role or tenant. In production, take the principal from your authentication
     * layer (for example Spring Security's {@code SecurityContextHolder}), never from the request.
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
     * Pages through the photos the principal may perform {@code action} on.
     *
     * <p><strong>Demo only:</strong> the principal comes from query parameters, as in
     * {@link #list}.
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
     * Shows the adapter's bulk-delete guard. Expected to return 409 and delete nothing.
     *
     * <p>The adapter throws {@link UnsupportedOperationException} when its Specification is
     * passed to {@code delete(Specification)} and the plan uses a relation mapping. This endpoint
     * returns that as 409. To delete permitted rows, call {@code findAll(spec)} and then
     * {@code deleteAllById(ids)}.
     *
     * <p><strong>Demo only:</strong> the principal comes from query parameters, as in
     * {@link #list}.
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
            // The guard did not fire, which is a bug.
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
