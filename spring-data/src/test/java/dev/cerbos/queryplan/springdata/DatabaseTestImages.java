package dev.cerbos.queryplan.springdata;

import org.testcontainers.utility.DockerImageName;

/**
 * Single source of truth for the database container images this harness runs the corpus against.
 *
 * <p><b>Why the digest.</b> A tag is mutable — the Postgres 16 and MySQL 8.4 tags are both
 * moving targets — so a tag-only pin records an intent, not a build. The adversarial suite is a
 * differential whose divergences are dialect behaviour (collation, decimal-vs-double arithmetic,
 * parameter typing), so "which build was this proved against" has to be answerable from the
 * repository alone. {@code conformance/scripts/validate-corpus.sh} asserts every service image
 * reference in the repository carries a tag <em>and</em> a digest.
 *
 * <p>{@link DockerImageName} puts everything before {@code @sha256:} into the repository part, so
 * a {@code repo:tag@digest} reference does not compare equal to the module's expected {@code repo}
 * and has to declare {@link DockerImageName#asCompatibleSubstituteFor(String)} explicitly. The
 * reference Docker pulls is unaffected.
 */
final class DatabaseTestImages {

    // One line each, deliberately: validate-corpus.sh reads image references line by line, and a
    // reference split across a concatenation reads to it as an unpinned tag.
    private static final String POSTGRES_REFERENCE = "postgres:16@sha256:95206741a5b214807675e14165369d05b93a9cf692223b616d07cca227e74b0b";
    private static final String MYSQL_REFERENCE = "mysql:8.4@sha256:b3b90af2a6552ae30c266fdb7d5dd55f3afb72404bb78d37fe8a23eb857fd3fb";

    static final DockerImageName POSTGRES =
            DockerImageName.parse(POSTGRES_REFERENCE).asCompatibleSubstituteFor("postgres");

    static final DockerImageName MYSQL =
            DockerImageName.parse(MYSQL_REFERENCE).asCompatibleSubstituteFor("mysql");

    private DatabaseTestImages() {}
}
