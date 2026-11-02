/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

rootProject.name = "cerbos-spring-data"

// Builds and tests against the jpa core in this checkout; the published POM names its coordinate
// (docs/adr/0013-spring-data-wraps-the-jpa-core.md).
includeBuild("../jpa")
