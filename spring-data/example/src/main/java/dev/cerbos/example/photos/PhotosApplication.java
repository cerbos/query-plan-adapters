/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.example.photos;

import dev.cerbos.example.CerbosClientConfig;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

/**
 * The photo-sharing application. Component scanning covers only this package, so the demo-domain
 * beans are not registered.
 */
@SpringBootApplication
@Import(CerbosClientConfig.class)
public class PhotosApplication {
    public static void main(String[] args) {
        SpringApplication.run(PhotosApplication.class, args);
    }
}
