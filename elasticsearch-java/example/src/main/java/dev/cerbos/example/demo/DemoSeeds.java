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
 * {@code demo/seeds.json}, parsed. The file is repository-controlled and checked by
 * {@code demo/scripts/validate-demo.sh}.
 *
 * @param principals the demo principals
 * @param applicationFilter the predicate the APPLICATION owns, never expressed in policy
 * @param documents the seed rows
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DemoSeeds(List<Principal> principals,
                        ApplicationFilter applicationFilter,
                        List<Document> documents) {

    /** A demo principal. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Principal(String id, List<String> roles) {}

    /**
     * The application's own predicate, {@code archived == false AND region == 'emea'}. Kept in the
     * corpus so {@code validate-demo.sh} can check that usage shape 5 needs both halves.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ApplicationFilter(boolean archived, String region) {}

    /** One seed row. {@code public} is a Java keyword, so it is renamed. */
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

    Principal principal(String id) {
        return principals.stream()
                .filter(p -> id.equals(p.id()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "demo/seeds.json declares no principal '" + id + "'"));
    }
}
