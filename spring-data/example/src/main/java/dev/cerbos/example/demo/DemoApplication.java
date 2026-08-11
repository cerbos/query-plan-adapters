package dev.cerbos.example.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import dev.cerbos.example.CerbosClientConfig;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * The demo-domain program: the second application in this example, and the one
 * {@code demo/scripts/run-example.sh spring-data} runs.
 *
 * <p>It takes no arguments, exercises the
 * <a href="https://github.com/cerbos/query-plan-adapters/issues/349">five shared usage
 * shapes</a> against {@code demo/}'s policies and seed rows, and prints exactly one JSON
 * document to stdout for the shared runner to diff against {@code demo/expected.json}. The
 * photo-sharing application it shares this source tree with is untouched and still the
 * onboarding artifact: the demo domain is a floor, not a ceiling
 * ({@code docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md}).
 *
 * <p>Component scanning is package-scoped, so this application sees only
 * {@code dev.cerbos.example.demo} — its own entity, repository and beans. The photo/album/
 * workspace entities are never registered here and the demo schema is the {@code documents}
 * table alone. The PDP client is the one thing the two applications share, and it is imported
 * by name rather than scanned.
 */
@SpringBootApplication
@Import(CerbosClientConfig.class)
public class DemoApplication {

    /**
     * The real stdout, captured before {@link #main} redirects {@link System#out}.
     *
     * <p>The contract is one JSON document on stdout and everything else on stderr, and a Spring
     * Boot application logs to stdout by default — as does anything on the classpath that
     * decides to print. Rather than configuring each of them, stdout is redirected to stderr for
     * the whole JVM and the one line that must reach the runner is written through this handle.
     * It holds under any logging configuration, and under a dependency that prints without one.
     */
    private static final PrintStream STDOUT = System.out;

    public static void main(String[] args) throws Exception {
        // Before anything can print, and before anything can connect: a misinvocation costs one
        // clear message rather than a Spring placeholder failure part-way through startup.
        System.setOut(System.err);
        Path demoDir = demoDir();
        requireCerbosHost();

        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(DemoApplication.class)
                .web(WebApplicationType.NONE)
                .bannerMode(Banner.Mode.OFF)
                .profiles("demo")
                .properties("demo.seeds=" + demoDir.resolve("seeds.json"))
                .run(args)) {

            Map<String, Object> document = Map.of(
                    // The runner checks this against the adapter it was asked for, so a stale
                    // build directory or a copied run.sh fails there instead of quietly passing
                    // on the shared expectations.
                    "adapter", "spring-data",
                    "shapes", context.getBean(DemoShapes.class).run());

            // Map keys sorted, which is the same canonical form demo/scripts/run-example.sh puts
            // both sides of its diff into (`jq -S`). Java's Map.of has no defined iteration
            // order, so without this the document a human reads on a failure is shuffled
            // differently on every run while the diff stays stable — the worst of both. Record
            // components are unaffected and stay in declaration order.
            STDOUT.println(new ObjectMapper()
                    .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(document));
            STDOUT.flush();
        }
    }

    /**
     * The shared corpus directory, passed as {@code -Ddemo.dir} by {@code run.sh}. The path
     * arithmetic lives there rather than here: how many directories up the repository root sits
     * is the launcher's business, not the application's.
     */
    private static Path demoDir() {
        String configured = System.getProperty("demo.dir");
        if (configured == null || configured.isBlank()) {
            throw new IllegalStateException(
                    "-Ddemo.dir is not set — run this program through"
                            + " demo/scripts/run-example.sh spring-data");
        }
        Path demoDir = Path.of(configured);
        if (!Files.isRegularFile(demoDir.resolve("seeds.json"))) {
            throw new IllegalStateException(
                    "-Ddemo.dir=" + configured + " does not contain seeds.json");
        }
        return demoDir;
    }

    /**
     * The runner sets {@code CERBOS_HOST}, and there is deliberately no fallback anywhere in
     * this example. The obvious default — Cerbos's own 3592/3593 — is what every adapter's
     * {@code cerbos run} test sidecar binds, so an unset {@code CERBOS_HOST} would not fail: it
     * would quietly plan against {@code ../../policies} and produce a diff against
     * {@code demo/expected.json} that reads as an adapter bug. Two other examples shipped that
     * exact default, which is why {@code demo/scripts/validate-demo.sh} now fails the build on a
     * hardcoded PDP address rather than trusting prose.
     */
    private static void requireCerbosHost() {
        String host = System.getenv("CERBOS_HOST");
        if (host == null || host.isBlank()) {
            throw new IllegalStateException(
                    "CERBOS_HOST is not set — run this program through"
                            + " demo/scripts/run-example.sh spring-data");
        }
    }

    @Bean
    DemoSeeds demoSeeds(@Value("${demo.seeds}") String seedsFile) {
        return DemoSeeds.read(Path.of(seedsFile));
    }
}
