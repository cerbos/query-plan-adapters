/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.CerbosClientBuilder;

import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * The pinned Cerbos PDP image for {@link ElasticsearchAdversarialConformanceTest}. The PDP produces
 * both the plan and the {@code check()} oracle, so {@link #assertPinned} checks the started
 * container runs the build {@code conformance/CERBOS_IMAGE_DIGEST} names.
 */
final class CerbosTestImage {

    /** System property that replaces the pinned image. No workflow sets it. */
    static final String OVERRIDE_PROPERTY = "cerbos.test.image";

    static final String IMAGE = System.getProperty(OVERRIDE_PROPERTY, defaultImage());

    /** True when a caller replaced the pinned oracle for this run. */
    static final boolean OVERRIDDEN = System.getProperty(OVERRIDE_PROPERTY) != null;

    // A stalled HTTP/2 stream fails the run instead of hanging the CI job.
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

    /** The digest from {@code conformance/CERBOS_IMAGE_DIGEST}. */
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
            // The tag names the release; the digest guards against the tag being re-pointed.
            return "ghcr.io/cerbos/cerbos:" + Files.readString(versionFile).strip()
                    + "@" + pinnedDigest();
        } catch (IOException e) {
            throw new ExceptionInInitializerError(
                    "Unable to read the pinned Cerbos image from " + conformance + ": " + e);
        }
    }

    /** The repo digests Docker records for a started container's image. */
    static List<String> resolvedDigests(GenericContainer<?> container) {
        List<String> digests = container.getDockerClient()
                .inspectImageCmd(container.getDockerImageName()).exec().getRepoDigests();
        return digests == null ? List.of() : digests;
    }

    /**
     * Fails unless the started container runs the pinned digest. With the override set, prints a
     * banner saying the run is not against the pinned PDP instead.
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
