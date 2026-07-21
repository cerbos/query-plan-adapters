package dev.cerbos.example.photos;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
public class SeedData implements CommandLineRunner {

    private final PhotoRepository repository;

    public SeedData(PhotoRepository repository) {
        this.repository = repository;
    }

    @Override
    public void run(String... args) {
        repository.saveAll(java.util.List.of(
                new Photo("p1", "alice",   "Beach sunset",   true,  false, "Lisbon",
                        Set.of("travel", "sunset")),
                new Photo("p2", "alice",   "Family lunch",   false, false, "Home",
                        Set.of("friends", "food")),
                new Photo("p3", "bob",     "Wedding",        true,  true,  "Paris",
                        Set.of("wedding")),
                new Photo("p4", "bob",     "Selfie",         false, false, "Studio",
                        Set.of("portrait")),
                new Photo("p5", "charlie", "Mountain hike",  true,  false, "Alps",
                        Set.of("travel", "outdoors", "friends")),
                new Photo("p6", "alice",   "Old archive",    false, true,  "Home",
                        Set.of("legacy"))
        ));
    }
}
