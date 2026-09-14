/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Pinned Cerbos image used by {@link ElasticsearchAdversarialConformanceTest}, the one suite here
 * that starts a PDP.
 *
 * <p>The PDP is the oracle for BOTH sides of the differential — it produces the plan under test
 * and the per-row {@code check()} decisions it is compared against — so which build answered is
 * the one fact a green run cannot be read without. {@link #assertPinned} is where that fact is
 * asserted rather than printed: the container's resolved digest must be the one
 * {@code conformance/CERBOS_IMAGE_DIGEST} names, unless the {@code cerbos.test.image} override is
 * set, in which case the run says so in a way nobody can miss.
 */
final class CerbosTestImage {

    /** The system property that swaps the oracle. Never set by any workflow. */
    static final String OVERRIDE_PROPERTY = "cerbos.test.image";

    static final String IMAGE = System.getProperty(OVERRIDE_PROPERTY, defaultImage());

    /** True when a caller replaced the pinned oracle for this run. */
    static final boolean OVERRIDDEN = System.getProperty(OVERRIDE_PROPERTY) != null;

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
        System.out.printf("==> Elasticsearch conformance PDP: %s (digest %s)%n", IMAGE, pinned);
    }
}
