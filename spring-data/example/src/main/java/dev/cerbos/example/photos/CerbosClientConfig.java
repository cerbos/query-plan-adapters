package dev.cerbos.example.photos;

import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.CerbosClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CerbosClientConfig {

    @Bean
    CerbosBlockingClient cerbosBlockingClient(
            @Value("${cerbos.host}") String host,
            @Value("${cerbos.port}") int port) throws CerbosClientBuilder.InvalidClientConfigurationException {
        return new CerbosClientBuilder(host + ":" + port)
                .withPlaintext()
                .buildBlockingClient();
    }
}
