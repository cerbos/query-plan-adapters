/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

rootProject.name = "cerbos-spring-data-photos-example"

// No includeBuild(".."): a composite build would bypass the published POM that this example tests.
// The adapter comes from mavenLocal instead (see build.gradle.kts); run.sh, scripts/smoke.sh and
// scripts/smoke-edge-cases.sh each publish it first.
