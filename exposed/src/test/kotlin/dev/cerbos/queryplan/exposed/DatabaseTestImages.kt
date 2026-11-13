package dev.cerbos.queryplan.exposed

import org.testcontainers.utility.DockerImageName
import java.nio.file.Files
import java.nio.file.Path

/**
 * Single source of truth for the database container images this harness replays the corpus
 * against, read from two pin files in `exposed/`: `POSTGRES_IMAGE` and `MYSQL_IMAGE`.
 *
 * A file rather than a constant so Renovate's `<SERVICE>_IMAGE` manager bumps it. The build declares
 * both files as inputs of `test`, so bumping one re-runs the suites rather than replaying a stale
 * pass.
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
