package dev.cerbos.queryplan.elasticsearch;

import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Pinned Cerbos image used by {@link ElasticsearchAdversarialConformanceTest}, the one suite here
 * that starts a PDP.
 */
final class CerbosTestImage {

    static final String IMAGE =
            System.getProperty("cerbos.test.image", defaultImage());

    private CerbosTestImage() {}

    private static String defaultImage() {
        Path conformance = Path.of(System.getProperty("user.dir"), "..", "conformance");
        Path versionFile = conformance.resolve("CERBOS_VERSION");
        Path digestFile = conformance.resolve("CERBOS_IMAGE_DIGEST");
        try {
            // Tag AND digest: the tag records which release this is, the digest makes the pin
            // immune to the tag being re-pointed. validate-corpus.sh asserts the two agree
            // everywhere they are restated.
            return "ghcr.io/cerbos/cerbos:" + Files.readString(versionFile).strip()
                    + "@" + Files.readString(digestFile).strip();
        } catch (IOException e) {
            throw new ExceptionInInitializerError(
                    "Unable to read the pinned Cerbos image from " + conformance + ": " + e);
        }
    }

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
