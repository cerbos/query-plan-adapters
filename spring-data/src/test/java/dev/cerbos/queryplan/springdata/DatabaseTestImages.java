package dev.cerbos.queryplan.springdata;

import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Single source of truth for the database container images this harness runs the corpus against,
 * read from two pin files in {@code spring-data/}: {@code POSTGRES_IMAGE} and {@code MYSQL_IMAGE}.
 *
 * <p><b>Why a file rather than a constant here.</b> A Java string constant is a pin nothing bumps:
 * {@code renovate.json}'s custom manager reads {@code <SERVICE>_IMAGE} files and nothing else
 * (its Dockerfile and compose managers are off), so a reference held in source stays wherever it
 * was last edited by hand. The same argument put the PostgreSQL pin in {@code pgx/POSTGRES_IMAGE}
 * and the Elasticsearch pins in {@code elasticsearch-java/ELASTICSEARCH_IMAGE}:
 * {@code conformance/scripts/validate-corpus.sh} scans {@code *_IMAGE} files, holds one digest
 * per tag, and nothing holds two tags equal — so a second spelling of a reference could be left
 * behind on an older server and stay green. The build declares both files as inputs of
 * {@code test}, so bumping one re-runs the suites rather than replaying a stale pass.
 *
 * <p><b>Why the digest.</b> A tag is mutable — the Postgres 16 and MySQL 8.4 tags are both
 * moving targets — so a tag-only pin records an intent, not a build. The adversarial suite is a
 * differential whose divergences are dialect behaviour (collation, decimal-vs-double arithmetic,
 * parameter typing), so "which build was this proved against" has to be answerable from the
 * repository alone. {@code validate-corpus.sh} asserts every service image reference in the
 * repository carries a tag <em>and</em> a digest.
 *
 * <p>{@link DockerImageName} puts everything before {@code @sha256:} into the repository part, so
 * a {@code repo:tag@digest} reference does not compare equal to the module's expected {@code repo}
 * and has to declare {@link DockerImageName#asCompatibleSubstituteFor(String)} explicitly. The
 * reference Docker pulls is unaffected.
 */
final class DatabaseTestImages {

    static final DockerImageName POSTGRES = read("POSTGRES_IMAGE", "postgres");

    static final DockerImageName MYSQL = read("MYSQL_IMAGE", "mysql");

    private DatabaseTestImages() {}

    private static DockerImageName read(String pinFile, String repository) {
        Path path = Path.of(System.getProperty("user.dir"), pinFile);
        try {
            String pinned = Files.readString(path).strip();
            if (!pinned.startsWith(repository + ":")) {
                throw new ExceptionInInitializerError(
                        path + " must pin " + repository + ", got: " + pinned);
            }
            // Which server a run proved the adapter against, in the log next to the PDP digest the
            // adversarial suite prints.
            System.out.printf("==> %s test image: %s (from %s)%n", repository, pinned, path);
            return DockerImageName.parse(pinned).asCompatibleSubstituteFor(repository);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(
                    "Unable to read the pinned " + repository + " image from " + path + ": " + e);
        }
    }
}
