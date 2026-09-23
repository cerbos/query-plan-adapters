/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

rootProject.name = "cerbos-elasticsearch-example"

// No includeBuild(".."): a composite build would bypass the published POM that this example tests.
// The adapter comes from mavenLocal instead (see build.gradle.kts).
