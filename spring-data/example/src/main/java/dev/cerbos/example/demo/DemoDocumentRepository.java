/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.example.demo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

/**
 * Runs the adapter's Specification through {@code JpaSpecificationExecutor}, paged and unpaged.
 */
public interface DemoDocumentRepository
        extends JpaRepository<DemoDocument, String>, JpaSpecificationExecutor<DemoDocument> {
}
