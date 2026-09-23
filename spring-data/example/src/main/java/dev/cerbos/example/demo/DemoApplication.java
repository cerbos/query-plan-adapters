/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

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
 * The demo-domain program run by {@code demo/scripts/run-example.sh spring-data}.
 *
 * <p>It runs the five shared usage shapes against {@code demo/}'s policies and seed rows and
 * prints one JSON document to stdout, which the runner diffs against {@code demo/expected.json}.
 * Component scanning covers only this package, so the photo-sharing entities are not registered.
 */
@SpringBootApplication
@Import(CerbosClientConfig.class)
public class DemoApplication {

    // The real stdout. main() sends System.out to stderr so logs and stray prints cannot mix
    // with the JSON document, which is written through this handle.
    private static final PrintStream STDOUT = System.out;

    public static void main(String[] args) throws Exception {
        // Redirect and validate before Spring starts, so a misinvocation fails with one clear
        // message.
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
                    // The runner checks this name, so output from the wrong example fails.
                    "adapter", "spring-data",
                    "shapes", context.getBean(DemoShapes.class).run());

            // Map.of has no stable iteration order, so sort keys to keep the output readable
            // and stable between runs. The runner compares with `jq -S` either way.
            STDOUT.println(new ObjectMapper()
                    .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(document));
            STDOUT.flush();
        }
    }

    // The shared demo/ directory, passed as -Ddemo.dir by run.sh.
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

    // No default address: Cerbos's default ports may belong to another local PDP with other
    // policies, and planning against it would look like an adapter bug.
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
