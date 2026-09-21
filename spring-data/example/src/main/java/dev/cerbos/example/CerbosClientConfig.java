package dev.cerbos.example;

import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.CerbosClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * The PDP client, shared by both applications in this example.
 *
 * <p>It sits in the parent package precisely so neither application's component scan reaches it —
 * each scans its own package — and both {@code @Import} it explicitly. One definition of how this
 * example connects to a PDP, rather than two waiting to disagree about it.
 */
@Configuration
public class CerbosClientConfig {

    // Bound stalled PDP calls in this example and in real applications. Healthy calls finish
    // in milliseconds; this generous deadline fails a stalled stream instead of hanging forever.
    private static final Duration CERBOS_CALL_TIMEOUT = Duration.ofSeconds(30);

    /**
     * {@code cerbos.address} is {@code ${CERBOS_HOST}} with no fallback — see the comment on it
     * in {@code application.yaml} for why a default would be worse than a failure to start.
     */
    @Bean
    CerbosBlockingClient cerbosBlockingClient(@Value("${cerbos.address}") String address)
            throws CerbosClientBuilder.InvalidClientConfigurationException {
        return new CerbosClientBuilder(address)
                .withPlaintext()
                .withTimeout(CERBOS_CALL_TIMEOUT)
                .buildBlockingClient();
    }
}
