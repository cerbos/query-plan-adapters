/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.CerbosClientBuilder;

import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * Pinned Cerbos image used by {@link AdversarialConformanceTest}, the one suite here that
 * starts a PDP. The other suites run offline.
 *
 * <p>The PDP is the oracle for BOTH sides of the differential — it produces the plan under test
 * and the per-row {@code check()} decisions it is compared against — so which build answered is
 * the one fact a green run cannot be read without. {@link #assertPinned} is where that fact is
 * asserted rather than printed: the container's resolved digest must be the one
 * {@code conformance/CERBOS_IMAGE_DIGEST} names, unless the {@code cerbos.test.image} override is
 * set, in which case the run says so in a way nobody can miss.
 *
 * <p><b>Why pinned.</b> Several tests pin planner-shape-dependent behavior (exact fail-closed
 * error strings for pass-through shapes, the {@code has()} over-grant tripwire,
 * differential-oracle row sets). A floating {@code :latest} tag makes past green runs
 * unreproducible once the tag moves and lets upstream planner changes flow into CI unnoticed.
 *
 * <p><b>Bump policy.</b> The pin lives in {@code conformance/CERBOS_VERSION} and
 * {@code conformance/CERBOS_IMAGE_DIGEST}, and bumping it is a deliberate, reviewed change:
 * update both halves, run the full suite, and re-evaluate every upstream-tracking test — in
 * particular {@code AdversarialConformanceTest.upstreamHasFoldOverGrantTripwire}, whose "fires
 * when upstream fixes the fold" property is dormant between bumps and only re-checks upstream
 * behavior when the pinned image moves. Override per-run with
 * {@code -Dcerbos.test.image=<image reference>} to trial a newer PDP without editing source.
 * (Spelled with a placeholder rather than a real reference on purpose: validate-corpus.sh scans
 * the whole repository for Cerbos image references and holds every one of them to the corpus
 * pin, including ones that only appear in documentation.)
 */
final class CerbosTestImage {

    /** The system property that swaps the oracle. Never set by any workflow. */
    static final String OVERRIDE_PROPERTY = "cerbos.test.image";

    static final String IMAGE = System.getProperty(OVERRIDE_PROPERTY, defaultImage());

    /** True when a caller replaced the pinned oracle for this run. */
    static final boolean OVERRIDDEN = System.getProperty(OVERRIDE_PROPERTY) != null;

    // A stalled HTTP/2 stream must fail the differential instead of hanging the CI job.
    // Healthy local calls complete in milliseconds; 30 seconds leaves ample startup/load margin.
    private static final Duration CALL_TIMEOUT = Duration.ofSeconds(30);

    static CerbosBlockingClient client(GenericContainer<?> container)
            throws CerbosClientBuilder.InvalidClientConfigurationException {
        return new CerbosClientBuilder(container.getHost() + ":" + container.getMappedPort(3593))
                .withPlaintext().withTimeout(CALL_TIMEOUT).buildBlockingClient();
    }

    static String strictEvaluation() {
        String strict = System.getProperty("adapter.test.strictEvaluation", "false");
        if (!strict.equals("false") && !strict.equals("true")) {
            throw new IllegalArgumentException("ADAPTER_TEST_STRICT_EVALUATION must be false or true");
        }
        return strict;
    }

    private CerbosTestImage() {}

    private static Path conformanceDir() {
        return Path.of(System.getProperty("user.dir"), "..", "conformance").normalize();
    }

    /** The digest half of the pin, exactly as {@code conformance/CERBOS_IMAGE_DIGEST} spells it. */
    static String pinnedDigest() {
        try {
            return Files.readString(conformanceDir().resolve("CERBOS_IMAGE_DIGEST")).strip();
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Unable to read the pinned Cerbos digest from " + conformanceDir() + ": " + e);
        }
    }

    private static String defaultImage() {
        Path conformance = conformanceDir();
        Path versionFile = conformance.resolve("CERBOS_VERSION");
        try {
            // Tag AND digest: the tag records which release this is, the digest makes the pin
            // immune to the tag being re-pointed. validate-corpus.sh asserts the two agree
            // everywhere they are restated.
            return "ghcr.io/cerbos/cerbos:" + Files.readString(versionFile).strip()
                    + "@" + pinnedDigest();
        } catch (IOException e) {
            throw new ExceptionInInitializerError(
                    "Unable to read the pinned Cerbos image from " + conformance + ": " + e);
        }
    }

    /** Every repo digest Docker records for the image a started container runs. */
    static List<String> resolvedDigests(GenericContainer<?> container) {
        List<String> digests = container.getDockerClient()
                .inspectImageCmd(container.getDockerImageName()).exec().getRepoDigests();
        return digests == null ? List.of() : digests;
    }

    /**
     * The started container runs the pinned build, or the run is loudly NOT the pinned run.
     *
     * <p>Without this the override property changed the oracle silently: the resolved digest was
     * printed, and a log line is not an assertion. A wrong digest here is a Docker cache holding
     * a different build under the pinned reference, or a pin whose two halves disagree — either
     * way the differential would be against a PDP nobody chose, so it fails rather than runs.
     */
    static void assertPinned(GenericContainer<?> container) {
        List<String> digests = resolvedDigests(container);
        if (OVERRIDDEN) {
            String banner = "=".repeat(78);
            System.out.printf("%s%n"
                            + "==> CERBOS PDP OVERRIDDEN via -D%s=%s%n"
                            + "==> This run is NOT against the pinned oracle"
                            + " (conformance/CERBOS_VERSION @ CERBOS_IMAGE_DIGEST).%n"
                            + "==> Resolved digests: %s%n"
                            + "==> Its result proves nothing about the pinned PDP.%n"
                            + "%s%n",
                    banner, OVERRIDE_PROPERTY, IMAGE, digests, banner);
            return;
        }
        String pinned = pinnedDigest();
        boolean matches = digests.stream().anyMatch(digest -> digest.endsWith("@" + pinned));
        if (!matches) {
            throw new AssertionError("the started Cerbos container does not run the pinned build:"
                    + " conformance/CERBOS_IMAGE_DIGEST is " + pinned + " but Docker resolved "
                    + IMAGE + " to " + digests);
        }
        System.out.printf("==> Adversarial-oracle Cerbos PDP: %s (digest %s)%n", IMAGE, pinned);
    }
}
