package dev.cerbos.example.demo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;

/**
 * {@code demo/seeds.json}, parsed. The rows every example application persists and the
 * principals every one of them plans for, read from the shared corpus rather than restated here
 * — a copy per example would be one more thing to update when the domain gains a row.
 *
 * <p>Corpus files are repository-controlled. Live case execution resolves every principal and
 * expected operation through this parsed data.
 *
 * @param principals the three demo principals
 * @param applicationFilter the predicate the APPLICATION owns, never expressed in policy
 * @param documents the eight seed rows
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DemoSeeds(List<Principal> principals,
                        ApplicationFilter applicationFilter,
                        List<Document> documents) {

    /** A demo principal: an id and the roles the policy's rules are keyed on. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Principal(String id, List<String> roles) {}

    /**
     * {@code archived == false AND region == 'emea'} — the application's own predicate. It lives
     * in the corpus so the canonical composed cases can prove the shape discriminates: applying
     * this predicate alone, or the adapter's filter
     * alone, must both give the wrong answer.
     */
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

    /** The named principal, or a failure naming the corpus — never a silently anonymous plan. */
    Principal principal(String id) {
        return principals.stream()
                .filter(p -> id.equals(p.id()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "demo/seeds.json declares no principal '" + id + "'"));
    }
}
