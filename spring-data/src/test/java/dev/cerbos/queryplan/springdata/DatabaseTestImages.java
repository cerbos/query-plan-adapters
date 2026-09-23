/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Database images for the adversarial suite, read from {@code spring-data/POSTGRES_IMAGE} and
 * {@code spring-data/MYSQL_IMAGE} so Renovate can bump them. Each reference carries a tag and a
 * digest; {@code validate-corpus.sh} checks both.
 *
 * <p>{@link DockerImageName} treats a {@code repo:tag@digest} reference as a different repository,
 * so it must be declared a compatible substitute for the module's expected image.
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
            // Log which server this run used.
            System.out.printf("==> %s test image: %s (from %s)%n", repository, pinned, path);
            return DockerImageName.parse(pinned).asCompatibleSubstituteFor(repository);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(
                    "Unable to read the pinned " + repository + " image from " + path + ": " + e);
        }
    }
}
