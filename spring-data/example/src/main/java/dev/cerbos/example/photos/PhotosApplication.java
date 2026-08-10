package dev.cerbos.example.photos;

import dev.cerbos.example.CerbosClientConfig;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

/**
 * Component scanning starts at this class's package, so this application sees the photo/album/
 * workspace beans and nothing from the demo-domain program beside it. The PDP client is the one
 * thing the two share, and it is imported by name rather than scanned.
 */
@SpringBootApplication
@Import(CerbosClientConfig.class)
public class PhotosApplication {
    public static void main(String[] args) {
        SpringApplication.run(PhotosApplication.class, args);
    }
}
