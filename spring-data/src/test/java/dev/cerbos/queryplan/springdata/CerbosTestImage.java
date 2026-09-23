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
 * The pinned Cerbos image for {@link AdversarialConformanceTest}, the only suite here that starts
 * a PDP. The pin is read from {@code conformance/CERBOS_VERSION} and
 * {@code conformance/CERBOS_IMAGE_DIGEST}.
 *
 * <p>Override for one run with {@code -Dcerbos.test.image=<image reference>}. Do not write a real
 * Cerbos image reference in a comment: {@code validate-corpus.sh} holds every one to the pin.
 */
final class CerbosTestImage {

    /** Replaces the pinned PDP for one run. No workflow sets it. */
    static final String OVERRIDE_PROPERTY = "cerbos.test.image";

    static final String IMAGE = System.getProperty(OVERRIDE_PROPERTY, defaultImage());

    /** True when a caller replaced the pinned oracle for this run. */
    static final boolean OVERRIDDEN = System.getProperty(OVERRIDE_PROPERTY) != null;

    // Fail a stalled call instead of hanging the CI job.
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
            // The digest protects against the tag being re-pointed.
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
     * Fails unless the container runs the pinned digest. With the override set, prints a banner
     * instead.
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
