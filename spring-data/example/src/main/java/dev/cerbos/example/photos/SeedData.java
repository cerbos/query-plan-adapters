package dev.cerbos.example.photos;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component
public class SeedData implements CommandLineRunner {

    private final PhotoRepository photoRepository;
    private final AlbumRepository albumRepository;
    private final WorkspaceRepository workspaceRepository;

    public SeedData(PhotoRepository photoRepository, AlbumRepository albumRepository,
                    WorkspaceRepository workspaceRepository) {
        this.photoRepository = photoRepository;
        this.albumRepository = albumRepository;
        this.workspaceRepository = workspaceRepository;
    }

    @Override
    public void run(String... args) {
        photoRepository.saveAll(List.of(
                new Photo("p1", "acme", "alice",   "Beach sunset",   true,  false, "Lisbon",
                        5, new PhotoDetails(4000, 3000), Set.of("travel", "sunset"))
                        .addLabel("editorial", 0.95, true)
                        .addGrant("acme", "view", null, null),
                new Photo("p2", "acme", "alice",   "Family lunch",   false, false, null,
                        3, new PhotoDetails(1600, 1200), Set.of("friends", "food"))
                        .addLabel("safety", 0.90, false)
                        .addLabel("safety", 0.85, false)
                        .addLabel("faces", 0.60, true)
                        .addGrant("acme", "view", null, null)
                        .addGrant("acme", "view", null, "acme:finance"),
                new Photo("p3", "acme", "bob",     "Wedding",        true,  true,  "Paris",
                        4, new PhotoDetails(6000, 4000), Set.of("wedding"))
                        .addLabel("safety", 0.95, true)
                        .addGrant("acme", "view", null, null)
                        .addGrant("acme", "view", null, "acme:sales")
                        .addGrant("acme", "view", "alice", null),
                new Photo("p4", "acme", "bob",     "Selfie",         false, false, "Studio",
                        2, new PhotoDetails(800, 600), Set.of("portrait"))
                        .addGrant("globex", "view", null, "acme:legal"),
                new Photo("p5", "acme", "charlie", "Mountain hike",  true,  false, "Alps",
                        5, new PhotoDetails(5000, 3500), Set.of("travel", "outdoors", "friends"))
                        .addLabel("quality", 0.90, true)
                        .addLabel("safety", 0.40, false)
                        .addGrant("acme", "view", null, "acme:engineering")
                        .addGrant("acme", "view", null, "acme:engineering"),
                new Photo("p6", "acme", "alice",   "Old archive",    false, true,  "Home",
                        1, new PhotoDetails(1024, 768), Set.of("legacy")),
                new Photo("p7", "acme", "dana",    "100% coverage",  true,  false, null,
                        4, new PhotoDetails(3200, 1800), Set.of())
                        .addGrant("acme", "edit", null, "acme:finance"),
                new Photo("p8", "acme", "erin",    "Under_score",    true,  false, "Tokyo",
                        4, new PhotoDetails(3200, 2400), Set.of("unicode", "旅行"))
                        .addGrant("acme", "view", "bob", null),
                new Photo("p9", "globex", "globex-user", "Confidential acquisition",
                        false, true, null, 2, new PhotoDetails(800, 600), Set.of("globex"))
                        .addGrant("globex", "view", null, "globex:finance")
        ));

        albumRepository.saveAll(List.of(
                new Album("a1", "acme", "alice", "Launch campaign", false, Set.of("bob")),
                new Album("a2", "acme", "bob", "Company offsite", true, Set.of("alice")),
                new Album("a3", "globex", "globex-user", "Acquisition room", true, Set.of()),
                new Album("a4", "acme", "charlie", "Board drafts", false, Set.of())
        ));

        workspaceRepository.saveAll(List.of(
                new Workspace("w1", "acme", "bob", "Production", true, Set.of("alice")),
                new Workspace("w2", "acme", "alice", "Suspended migration", false,
                        Set.of("charlie")),
                new Workspace("w3", "globex", "globex-user", "M&A", true, Set.of("alice")),
                new Workspace("w4", "acme", "charlie", "Finance", true, Set.of())
        ));
    }
}
