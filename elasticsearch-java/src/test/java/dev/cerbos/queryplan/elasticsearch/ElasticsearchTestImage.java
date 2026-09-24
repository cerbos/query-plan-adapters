/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The Elasticsearch image both container-backed suites start, read from a pin file in
 * {@code elasticsearch-java/}: {@code ELASTICSEARCH_IMAGE} by default, or the file named by the
 * {@code elasticsearch.test.image.file} system property (the workflow uses this to select
 * {@code ELASTICSEARCH_NEXT_IMAGE}). The pin is a file so {@code example/run.sh} can read it too.
 *
 * <p>Pinning by digest has two Testcontainers side effects:
 *
 * <ul>
 *   <li>{@link DockerImageName} treats everything before {@code @sha256:} as the repository, so
 *       the image must declare {@link DockerImageName#asCompatibleSubstituteFor(String)}.
 *   <li>{@code ElasticsearchContainer} reads the version from the digest and takes its pre-8
 *       branch, so it sets no {@code ELASTIC_PASSWORD} or CA cert. Both suites disable security
 *       and use plain HTTP, so this does not matter to them.
 * </ul>
 */
final class ElasticsearchTestImage {

    private static final String REPOSITORY = "docker.elastic.co/elasticsearch/elasticsearch";

    static final String PIN_FILE =
            System.getProperty("elasticsearch.test.image.file", "ELASTICSEARCH_IMAGE");

    static final DockerImageName IMAGE =
            DockerImageName.parse(reference()).asCompatibleSubstituteFor(REPOSITORY);

    private ElasticsearchTestImage() {}

    private static String reference() {
        Path pinFile = Path.of(System.getProperty("user.dir"), PIN_FILE);
        try {
            String pinned = Files.readString(pinFile).strip();
            if (!pinned.startsWith(REPOSITORY + ":")) {
                throw new ExceptionInInitializerError(
                        pinFile + " must pin " + REPOSITORY + ", got: " + pinned);
            }
            // Log which server this run used; the workflow's matrix legs differ only in this.
            System.out.printf("==> Elasticsearch test image: %s (from %s)%n", pinned, pinFile);
            return pinned;
        } catch (IOException e) {
            throw new ExceptionInInitializerError(
                    "Unable to read the pinned Elasticsearch image from " + pinFile + ": " + e);
        }
    }
}
