package dev.cerbos.queryplan.springdata;

import org.testcontainers.containers.GenericContainer;

import java.util.List;

/**
 * Single source of truth for the Cerbos PDP container image used by the Testcontainers
 * suites ({@link SpringDataIntegrationTest} and {@link AdversarialConformanceTest}).
 *
 * <p><b>Why pinned.</b> Several tests pin planner-shape-dependent behavior (exact
 * fail-closed error strings for pass-through shapes, the {@code has()} over-grant
 * tripwire, differential-oracle row sets). A floating {@code :latest} tag makes past
 * green runs unreproducible once the tag moves and lets upstream planner changes flow
 * into CI unnoticed. The image is therefore pinned to an explicit release and bumped
 * deliberately (repo precedent: the shared conformance corpus pins 0.54.0).
 *
 * <p><b>Bump policy.</b> Bumping the default below is a deliberate, reviewed change:
 * update the version, run the full suite, and re-evaluate every upstream-tracking test —
 * in particular {@code AdversarialConformanceTest.upstreamHasFoldOverGrantTripwire},
 * whose "fires when upstream fixes the fold" property is dormant between bumps and only
 * re-checks upstream behavior when the pinned image moves. Override per-run with
 * {@code -Dcerbos.test.image=ghcr.io/cerbos/cerbos:<tag>} to trial a newer PDP without
 * editing source.
 */
final class CerbosTestImage {

    /** Pinned Cerbos PDP image; override with the {@code cerbos.test.image} system property. */
    static final String IMAGE =
            System.getProperty("cerbos.test.image", "ghcr.io/cerbos/cerbos:0.54.0");

    private CerbosTestImage() {}

    /**
     * Best-effort repo digest of the image backing a started container, so CI logs record
     * exactly which PDP build produced a run even if the tag is later re-pointed.
     */
    static String resolvedDigest(GenericContainer<?> container) {
        try {
            List<String> digests = container.getDockerClient()
                    .inspectImageCmd(container.getDockerImageName()).exec().getRepoDigests();
            return digests == null || digests.isEmpty() ? "<no digest>" : digests.get(0);
        } catch (RuntimeException e) {
            return "<digest unavailable: " + e.getMessage() + ">";
        }
    }
}
