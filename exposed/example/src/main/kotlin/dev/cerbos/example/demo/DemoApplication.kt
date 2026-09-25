@file:JvmName("DemoApplication")

package dev.cerbos.example.demo

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import dev.cerbos.sdk.CerbosBlockingClient
import dev.cerbos.sdk.CerbosClientBuilder
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.transactions.transaction

/**
 * The real stdout, captured before [main] redirects `System.out`.
 *
 * The contract is one JSON document on stdout and everything else on stderr. Exposed, gRPC, SLF4J
 * and Jackson all sit on this classpath and any of them is free to print without a logging
 * configuration. Rather than configuring each of them, stdout is redirected to stderr for the
 * whole JVM and the one line that must reach the runner is written through this handle. It holds
 * under any logging configuration, and under a dependency that prints without one.
 *
 * A file-level property rather than a local: it is initialised when this file's class loads, which
 * is before the first statement of [main].
 */
private val REAL_STDOUT: PrintStream = System.out

/**
 * The exposed example: the program `demo/scripts/run-example.sh exposed` runs.
 *
 * It takes no arguments, exercises the
 * [five shared usage shapes](https://github.com/cerbos/query-plan-adapters/issues/349) against
 * `demo/`'s policies and seed rows, and prints exactly one JSON document to stdout for the shared
 * runner to diff against `demo/expected.json`.
 *
 * Two things it is handed, and where each comes from:
 *
 * - `CERBOS_HOST` — the environment variable the shared runner sets. There is deliberately no
 *   fallback; see [cerbosHost].
 * - `-Ddemo.dir` — the shared corpus directory. The path arithmetic lives in `run.sh`: how many
 *   directories up the repository root sits is the launcher's business, not the application's.
 *
 * The store is H2 in memory, inside this JVM, so there is nothing for `run.sh` to start.
 */
fun main() {
    // Before anything can print, and before anything can connect: a misinvocation costs one clear
    // message rather than a connection failure part-way through seeding.
    System.setOut(System.err)

    val demoDir = demoDir()
    val cerbos = cerbosClient(cerbosHost())
    val seeds = DemoSeeds.read(demoDir.resolve("seeds.json"))

    // `DB_CLOSE_DELAY=-1` keeps the in-memory database alive between connections, so the schema
    // this program creates survives for every query it then runs.
    val database = Database.connect(
        url = "jdbc:h2:mem:cerbos_exposed_demo;DB_CLOSE_DELAY=-1",
        driver = "org.h2.Driver",
    )

    // One transaction around every shape. The adapter returns an `Op<Boolean>`, a value that
    // renders inside the CALLER's transaction for that transaction's dialect, so the transaction
    // is the application's to open — which is the half of the contract an example should show.
    val shapes = transaction(database) { DemoShapes(cerbos, seeds).run() }

    val document = linkedMapOf(
        // The runner checks this against the adapter it was asked for, so a stale build directory
        // or a copied run.sh fails there instead of quietly passing on the shared expectations.
        "adapter" to "exposed",
        "shapes" to shapes,
    )

    // Map keys sorted, which is the same canonical form demo/scripts/run-example.sh puts both
    // sides of its diff into (`jq -S`), so the document a human reads on a failure is in the order
    // the diff is. Data class properties are unaffected and stay in declaration order.
    REAL_STDOUT.println(
        ObjectMapper()
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .writerWithDefaultPrettyPrinter()
            .writeValueAsString(document),
    )
    REAL_STDOUT.flush()
}

/**
 * Every call is bounded. A blocking gRPC stub with no deadline waits for ever, so a stalled stream
 * would hold this program, and the CI job running it, until something outside killed it. A real
 * application wants a deadline on its authorization calls for the same reason.
 */
private fun cerbosClient(host: String): CerbosBlockingClient =
    CerbosClientBuilder(host)
        .withPlaintext()
        .withTimeout(Duration.ofSeconds(30))
        .buildBlockingClient()

/** The shared corpus directory, passed as `-Ddemo.dir` by `run.sh`. */
private fun demoDir(): Path {
    val configured = System.getProperty("demo.dir")
    check(!configured.isNullOrBlank()) {
        "-Ddemo.dir is not set — run this program through demo/scripts/run-example.sh exposed"
    }
    val demoDir = Path.of(configured)
    check(Files.isRegularFile(demoDir.resolve("seeds.json"))) {
        "-Ddemo.dir=$configured does not contain seeds.json"
    }
    return demoDir
}

/**
 * The runner sets `CERBOS_HOST`, and there is deliberately no fallback anywhere in this example.
 * The obvious default — Cerbos's own 3592/3593 — is what every adapter's `cerbos run` test sidecar
 * binds, so an unset `CERBOS_HOST` would not fail: it would quietly plan against whichever policy
 * suite that sidecar serves, and produce a diff against `demo/expected.json` that reads as an
 * adapter bug. Two examples shipped that exact default, which is why
 * `demo/scripts/validate-demo.sh` now fails the build on a hardcoded PDP address rather than
 * trusting prose.
 */
private fun cerbosHost(): String {
    val host = System.getenv("CERBOS_HOST")
    check(!host.isNullOrBlank()) {
        "CERBOS_HOST is not set — run this program through demo/scripts/run-example.sh exposed"
    }
    return host
}
