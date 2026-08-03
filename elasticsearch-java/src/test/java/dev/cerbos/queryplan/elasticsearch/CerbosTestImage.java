package dev.cerbos.queryplan.elasticsearch;

import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Pinned Cerbos image used by Elasticsearch integration tests. */
final class CerbosTestImage {

    static final String IMAGE =
            System.getProperty("cerbos.test.image", defaultImage());

    private CerbosTestImage() {}

    private static String defaultImage() {
        Path versionFile = Path.of(
                System.getProperty("user.dir"), "..", "conformance", "CERBOS_VERSION");
        try {
            return "ghcr.io/cerbos/cerbos:" + Files.readString(versionFile).strip();
        } catch (IOException e) {
            throw new ExceptionInInitializerError(
                    "Unable to read pinned Cerbos version from " + versionFile + ": " + e);
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
