/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

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
import java.time.Duration;
import java.util.Map;

/**
 * Runs the five demo usage shapes and prints one JSON document to stdout, which
 * {@code demo/scripts/run-example.sh elasticsearch-java} diffs against {@code demo/expected.json}.
 *
 * <p>Inputs, all set by {@code run.sh}: the {@code CERBOS_HOST} environment variable, and the
 * {@code demo.dir}, {@code elasticsearch.url} and {@code adapter.dir} system properties.
 */
public final class DemoApplication {

    // Fail a stalled PDP call instead of hanging. Healthy calls take milliseconds.
    private static final Duration CERBOS_CALL_TIMEOUT = Duration.ofSeconds(30);

    /**
     * The real stdout. {@link #main} sends {@link System#out} to stderr so that only the JSON
     * document, written here, reaches stdout whatever the libraries print.
     */
    private static final PrintStream STDOUT = System.out;

    private DemoApplication() {}

    public static void main(String[] args) throws Exception {
        System.setOut(System.err);

        Path demoDir = demoDir();
        String cerbosHost = cerbosHost();
        String elasticsearchUrl = requiredProperty("elasticsearch.url");
        assertAdapterCameFromThePublishedArtifact();

        DemoSeeds seeds = DemoSeeds.read(demoDir.resolve("seeds.json"));

        // CerbosBlockingClient is not AutoCloseable; the JVM exit closes its channel.
        CerbosBlockingClient cerbos = cerbosClient(cerbosHost);

        try (DemoIndex index = new DemoIndex(elasticsearchUrl)) {
            Map<String, Object> document = Map.of(
                    // The runner checks this matches the adapter it was asked to run.
                    "adapter", "elasticsearch-java",
                    "shapes", new DemoShapes(cerbos, index, seeds).run());

            // Sort map keys so the output is stable; Map.of has no defined iteration order.
            STDOUT.println(new ObjectMapper()
                    .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(document));
            STDOUT.flush();
        }
    }

    private static CerbosBlockingClient cerbosClient(String host)
            throws CerbosClientBuilder.InvalidClientConfigurationException {
        return new CerbosClientBuilder(host).withPlaintext().withTimeout(CERBOS_CALL_TIMEOUT).buildBlockingClient();
    }

    private static Path demoDir() {
        Path demoDir = Path.of(requiredProperty("demo.dir"));
        if (!Files.isRegularFile(demoDir.resolve("seeds.json"))) {
            throw new IllegalStateException(
                    "-Ddemo.dir=" + demoDir + " does not contain seeds.json");
        }
        return demoDir;
    }

    /**
     * No default address: a default would silently reach any other local PDP, with different
     * policies. {@code demo/scripts/validate-demo.sh} rejects a hardcoded PDP address.
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
     * Fails unless the adapter was loaded from the published jar. A composite build
     * ({@code includeBuild("..")}) or a project dependency would substitute the local project, and
     * every shape would still pass without the POM ever being resolved.
     *
     * <p>A class directory fails the jar check; a jar under the adapter directory (its own
     * {@code build/libs}, or copies such as {@code installDist} makes under {@code example/build})
     * fails the location check.
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
