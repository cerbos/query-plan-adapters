package dev.cerbos.example.photos;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface PhotoRepository
        extends JpaRepository<Photo, String>, JpaSpecificationExecutor<Photo> {
}
