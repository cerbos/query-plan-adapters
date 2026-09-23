/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.example.photos;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
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

        // Rows for the edge-* rules in policies/photo.yaml, asserted by
        // scripts/smoke-edge-cases.sh. The "edge" tenant keeps them out of scripts/smoke.sh.
        Instant oldEnough = Instant.now().minus(30, ChronoUnit.DAYS);   // outside 24h window
        Instant tooRecent = Instant.now().minus(1, ChronoUnit.HOURS);   // inside 24h window
        photoRepository.saveAll(List.of(
                // e1: literal "[SEC]" prefix; must match edge-bracket-title.
                new Photo("e1", "edge", "edge-user", "[SEC] Quarterly report", true, false,
                        null, 3, new PhotoDetails(1000, 800), Set.of())
                        .withCreatedAt(oldEnough),
                // e2: 'S' is in the SQL Server character class [SEC], so an unescaped pattern
                // would match it there. Must not match edge-bracket-title.
                new Photo("e2", "edge", "edge-user", "Secret launch plan", false, false,
                        null, 3, new PhotoDetails(1000, 800), Set.of())
                        .withCreatedAt(tooRecent),
                // e3: -0.6 solves `score + 0.7 == 0.1` algebraically, but not in IEEE doubles,
                // so edge-ieee-eq must not match it.
                new Photo("e3", "edge", "edge-user", "Precision probe", true, false,
                        null, 3, new PhotoDetails(1000, 800), Set.of())
                        .withScore(-0.6)
                        .withCreatedAt(tooRecent),
                // e4: another non-null score; edge-ieee-ne matches it and e3.
                new Photo("e4", "edge", "edge-user", "Cold archive shot", false, false,
                        null, 3, new PhotoDetails(1000, 800), Set.of())
                        .withScore(2.5)
                        .withCreatedAt(oldEnough),
                // e5/e6: for edge-retention. e5 is older than 24h, e6 is not.
                new Photo("e5", "edge", "edge-user", "Retention candidate", true, false,
                        null, 3, new PhotoDetails(1000, 800), Set.of())
                        .withCreatedAt(oldEnough),
                new Photo("e6", "edge", "edge-user", "Fresh upload", true, false,
                        null, 3, new PhotoDetails(1000, 800), Set.of())
                        .withCreatedAt(tooRecent)
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
