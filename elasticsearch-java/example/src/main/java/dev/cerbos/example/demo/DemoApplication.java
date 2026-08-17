package dev.cerbos.example.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.CerbosClientBuilder;

import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.Map;

/**
 * The elasticsearch-java example: the program {@code demo/scripts/run-example.sh elasticsearch-java}
 * runs.
 *
 * <p>It takes no arguments, exercises the
 * <a href="https://github.com/cerbos/query-plan-adapters/issues/349">five shared usage shapes</a>
 * against {@code demo/}'s policies and seed rows, and prints exactly one JSON document to stdout for
 * the shared runner to diff against {@code demo/expected.json}.
 *
 * <p>Three things it is handed, and where each comes from:
 *
 * <ul>
 *   <li>{@code CERBOS_HOST} — the environment variable the shared runner sets. There is deliberately
 *       no fallback; see {@link #cerbosHost()}.</li>
 *   <li>{@code -Ddemo.dir} — the shared corpus directory. The path arithmetic lives in
 *       {@code run.sh}: how many directories up the repository root sits is the launcher's business,
 *       not the application's.</li>
 *   <li>{@code -Delasticsearch.url} — the store {@code run.sh} started. Also the launcher's, and for
 *       a sharper reason: {@code run.sh} owns the container and publishes the port, so a second
 *       spelling of the address in here would be a constant nothing holds equal to the first.</li>
 * </ul>
 */
public final class DemoApplication {

    /**
     * The real stdout, captured before {@link #main} redirects {@link System#out}.
     *
     * <p>The contract is one JSON document on stdout and everything else on stderr. Nothing here logs
     * to stdout today, but the Elasticsearch REST client, gRPC and Jackson all sit on this classpath
     * and any of them is free to print without a logging configuration. Rather than configuring each,
     * stdout is redirected to stderr for the whole JVM and the one line that must reach the runner is
     * written through this handle.
     */
    private static final PrintStream STDOUT = System.out;

    private DemoApplication() {}

    public static void main(String[] args) throws Exception {
        // Before anything can print, and before anything can connect: a misinvocation costs one clear
        // message rather than a connection failure part-way through seeding.
        System.setOut(System.err);

        Path demoDir = demoDir();
        String cerbosHost = cerbosHost();
        String elasticsearchUrl = requiredProperty("elasticsearch.url");
        assertAdapterCameFromThePublishedArtifact();

        DemoSeeds seeds = DemoSeeds.read(demoDir.resolve("seeds.json"));

        // CerbosBlockingClient is not AutoCloseable, so only the store is closed here. The JVM exits
        // immediately after the document is written, which takes the gRPC channel with it.
        CerbosBlockingClient cerbos = cerbosClient(cerbosHost);

        try (DemoIndex index = new DemoIndex(elasticsearchUrl)) {
            Map<String, Object> document = Map.of(
                    // The runner checks this against the adapter it was asked for, so a stale build
                    // directory or a copied run.sh fails there instead of quietly passing on the
                    // shared expectations.
                    "adapter", "elasticsearch-java",
                    "shapes", new DemoShapes(cerbos, index, seeds).run());

            // Map keys sorted, which is the same canonical form demo/scripts/run-example.sh puts both
            // sides of its diff into (`jq -S`). Java's Map.of has no defined iteration order, so
            // without this the document a human reads on a failure is shuffled differently on every
            // run while the diff stays stable — the worst of both. Record components are unaffected
            // and stay in declaration order.
            STDOUT.println(new ObjectMapper()
                    .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(document));
            STDOUT.flush();
        }
    }

    private static CerbosBlockingClient cerbosClient(String host)
            throws CerbosClientBuilder.InvalidClientConfigurationException {
        return new CerbosClientBuilder(host).withPlaintext().buildBlockingClient();
    }

    /**
     * The shared corpus directory, passed as {@code -Ddemo.dir} by {@code run.sh}.
     */
    private static Path demoDir() {
        Path demoDir = Path.of(requiredProperty("demo.dir"));
        if (!Files.isRegularFile(demoDir.resolve("seeds.json"))) {
            throw new IllegalStateException(
                    "-Ddemo.dir=" + demoDir + " does not contain seeds.json");
        }
        return demoDir;
    }

    /**
     * The runner sets {@code CERBOS_HOST}, and there is deliberately no fallback anywhere in this
     * example. The obvious default — Cerbos's own 3592/3593 — is what every adapter's
     * {@code cerbos run} test sidecar binds, so an unset {@code CERBOS_HOST} would not fail: it would
     * quietly plan against whichever policy suite that sidecar serves, and produce a diff against
     * {@code demo/expected.json} that reads as an adapter bug. Two examples shipped that exact
     * default, which is why {@code demo/scripts/validate-demo.sh} now fails the build on a hardcoded
     * PDP address rather than trusting prose.
     */
    private static String cerbosHost() {
        String host = System.getenv("CERBOS_HOST");
        if (host == null || host.isBlank()) {
            throw new IllegalStateException(
                    "CERBOS_HOST is not set — run this program through"
                            + " demo/scripts/run-example.sh elasticsearch-java");
        }
        return host;
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "-D" + name + " is not set — run this program through"
                            + " demo/scripts/run-example.sh elasticsearch-java");
        }
        return value;
    }

    /**
     * Asserts the adapter class this program just linked against came out of the published jar and
     * not out of the adapter's own build directory.
     *
     * <p>This is the Java analogue of the {@code sys.path} guard in {@code sqlalchemy/example/}, and
     * it exists because the failure it catches is silent. A single line in
     * {@code settings.gradle.kts} — {@code includeBuild("..")} — turns the declared coordinate into a
     * Gradle composite build, which substitutes the adapter's local project for it. Everything still
     * compiles, every shape still passes, and the POM and module metadata this example exists to
     * execute are never resolved at all
     * ({@code docs/adr/0002-examples-install-the-packed-artifact.md}).
     *
     * <p>Two conditions, because the substitutions do not all look the same. A composite build puts
     * the adapter's own {@code build/libs/*.jar} on the classpath, which is a jar; a project
     * dependency or a source set puts {@code build/classes/java/main}, which is a directory. Naming
     * the adapter directory rather than asserting a location inside the local Maven repository keeps
     * this independent of where that repository is configured to live — and it covers one more case
     * for free, since {@code example/build} is under it too: a launcher that COPIES the resolved jars
     * somewhere before running (which is exactly what the {@code application} plugin's
     * {@code installDist} does) would erase the distinction this checks, and is caught here rather
     * than passing.
     */
    private static void assertAdapterCameFromThePublishedArtifact() {
        CodeSource source =
                ElasticsearchQueryPlanAdapter.class.getProtectionDomain().getCodeSource();
        if (source == null || source.getLocation() == null) {
            throw new IllegalStateException(
                    "cannot tell where dev.cerbos:cerbos-elasticsearch was loaded from");
        }
        Path location = Path.of(URI.create(source.getLocation().toString())).toAbsolutePath()
                .normalize();
        Path adapterDir = Path.of(requiredProperty("adapter.dir")).toAbsolutePath().normalize();

        if (!location.getFileName().toString().endsWith(".jar")) {
            throw new IllegalStateException(
                    "the adapter was loaded from " + location + ", which is not a jar — this example"
                            + " must execute the published artifact"
                            + " (docs/adr/0002-examples-install-the-packed-artifact.md)");
        }
        if (location.startsWith(adapterDir)) {
            throw new IllegalStateException(
                    "the adapter was loaded from " + location + ", inside its own build directory —"
                            + " the declared coordinate has been substituted with the local project,"
                            + " so neither its POM nor its Gradle module metadata was resolved"
                            + " (docs/adr/0002-examples-install-the-packed-artifact.md)");
        }
        System.err.println("==> dev.cerbos:cerbos-elasticsearch resolved from " + location);
    }
}
