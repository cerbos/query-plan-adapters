package dev.cerbos.example;

import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.CerbosClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The PDP client, shared by both applications in this example.
 *
 * <p>It sits in the parent package precisely so neither application's component scan reaches it —
 * each scans its own package — and both {@code @Import} it explicitly. One definition of how this
 * example connects to a PDP, rather than two waiting to disagree about it.
 */
@Configuration
public class CerbosClientConfig {

    /**
     * {@code cerbos.address} is {@code ${CERBOS_HOST}} with no fallback — see the comment on it
     * in {@code application.yaml} for why a default would be worse than a failure to start.
     */
    @Bean
    CerbosBlockingClient cerbosBlockingClient(@Value("${cerbos.address}") String address)
            throws CerbosClientBuilder.InvalidClientConfigurationException {
        return new CerbosClientBuilder(address)
                .withPlaintext()
                .buildBlockingClient();
    }
}
