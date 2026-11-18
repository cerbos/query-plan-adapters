/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

// The domain stress harness for the spring-data adapter, built from ../../../../spring-data, which
// itself builds the jpa core from ../../../../jpa.
rootProject.name = "cerbos-spring-data-domains"
includeBuild("../../../../jpa")
includeBuild("../../../../spring-data")
