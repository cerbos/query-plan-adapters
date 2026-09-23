/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.example;

import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.CerbosClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * The PDP client shared by both applications in this example.
 *
 * <p>It lives in the parent package so neither application's component scan picks it up; both
 * {@code @Import} it instead.
 */
@Configuration
public class CerbosClientConfig {

    // Fails a stalled PDP call instead of hanging. Healthy calls take milliseconds.
    private static final Duration CERBOS_CALL_TIMEOUT = Duration.ofSeconds(30);

    // cerbos.address has no default; see application.yaml.
    @Bean
    CerbosBlockingClient cerbosBlockingClient(@Value("${cerbos.address}") String address)
            throws CerbosClientBuilder.InvalidClientConfigurationException {
        return new CerbosClientBuilder(address)
                .withPlaintext()
                .withTimeout(CERBOS_CALL_TIMEOUT)
                .buildBlockingClient();
    }
}
