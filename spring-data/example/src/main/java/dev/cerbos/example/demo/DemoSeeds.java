/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.example.demo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;

/**
 * {@code demo/seeds.json}, parsed.
 *
 * @param principals the demo principals
 * @param applicationFilter the application's own predicate, which no policy expresses
 * @param documents the seed rows
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DemoSeeds(List<Principal> principals,
                        ApplicationFilter applicationFilter,
                        List<Document> documents) {

    /** A demo principal: an id and the roles the policy's rules are keyed on. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Principal(String id, List<String> roles) {}

    /** The application's own predicate: {@code archived} and {@code region} must equal these. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ApplicationFilter(boolean archived, String region) {}

    /** One seed row. {@code public} is a Java keyword, hence the rename. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Document(String id, String ownerId, @JsonProperty("public") boolean isPublic,
                           String region, boolean archived) {}

    static DemoSeeds read(Path seedsFile) {
        try {
            return new ObjectMapper().readValue(seedsFile.toFile(), DemoSeeds.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read the demo corpus at " + seedsFile, e);
        }
    }

    /** The named principal; throws if the corpus has no such principal. */
    Principal principal(String id) {
        return principals.stream()
                .filter(p -> id.equals(p.id()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "demo/seeds.json declares no principal '" + id + "'"));
    }
}
