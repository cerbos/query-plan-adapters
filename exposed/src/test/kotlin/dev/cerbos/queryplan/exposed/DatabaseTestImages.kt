package dev.cerbos.queryplan.exposed

import org.testcontainers.utility.DockerImageName
import java.nio.file.Files
import java.nio.file.Path

/**
 * Single source of truth for the database container images this harness replays the corpus
 * against, read from two pin files in `exposed/`: `POSTGRES_IMAGE` and `MYSQL_IMAGE`.
 *
 * **Why a file rather than a constant here.** A Kotlin constant is a pin nothing bumps:
 * `renovate.json`'s custom manager reads `<SERVICE>_IMAGE` files and nothing else (its Dockerfile
 * and compose managers are off), so a reference held in source stays wherever it was last edited by
 * hand. The same argument put the PostgreSQL pin in `pgx/POSTGRES_IMAGE` and the Elasticsearch pins
 * in `elasticsearch-java/ELASTICSEARCH_IMAGE`: `conformance/scripts/validate-corpus.sh` scans
 * `*_IMAGE` files, holds one digest per tag, and nothing holds two tags equal — so a second
 * spelling of a reference could be left behind on an older server and stay green. The build declares
 * both files as inputs of `test`, so bumping one re-runs the suites rather than replaying a stale
 * pass.
 *
 * That one-digest-per-tag rule is also why these two files carry the SAME digests spring-data's do.
 * Both adapters prove the same servers, so a second digest under the same PostgreSQL tag would be
 * a claim that they are different builds. (The tag is deliberately not spelled out here:
 * `validate-corpus.sh` reads comments too, and a bare tag in one is an unpinned reference.)
 *
 * **Why the digest.** A tag is mutable — the Postgres 16 and MySQL 8.4 tags are both moving targets
 * — so a tag-only pin records an intent, not a build. This suite is a differential whose divergences
 * are dialect behaviour (collation, decimal-versus-double arithmetic, parameter typing), so "which
 * build was this proved against" has to be answerable from the repository alone.
 *
 * [DockerImageName] puts everything before `@sha256:` into the repository part, so a
 * `repo:tag@digest` reference does not compare equal to the module's expected `repo` and has to
 * declare [DockerImageName.asCompatibleSubstituteFor] explicitly. The reference Docker pulls is
 * unaffected.
 */
internal object DatabaseTestImages {

    val POSTGRES: DockerImageName by lazy { read("POSTGRES_IMAGE", "postgres") }

    val MYSQL: DockerImageName by lazy { read("MYSQL_IMAGE", "mysql") }

    private fun read(pinFile: String, repository: String): DockerImageName {
        val path = Path.of(System.getProperty("user.dir"), pinFile)
        val pinned = Files.readString(path).trim()
        check(pinned.startsWith("$repository:")) { "$path must pin $repository, got: $pinned" }
        // Which server a run proved the adapter against, in the log next to the PDP digest.
        println("==> $repository test image: $pinned (from $path)")
        return DockerImageName.parse(pinned).asCompatibleSubstituteFor(repository)
    }
}
