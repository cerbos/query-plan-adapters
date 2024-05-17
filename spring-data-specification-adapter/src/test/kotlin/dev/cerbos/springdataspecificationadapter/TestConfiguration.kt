package dev.cerbos.springdataspecificationadapter

import dev.cerbos.sdk.CerbosBlockingClient
import dev.cerbos.sdk.CerbosClientBuilder
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean

@TestConfiguration
class TestConfig {


    @Bean
    fun mockPrincipalRepo(): MockPrincipalRepository = MockPrincipalRepository()

    @Bean
    fun cerbos(): CerbosBlockingClient = CerbosClientBuilder("localhost:3593").withPlaintext().withInsecure().buildBlockingClient()

    @Bean
    fun resourceSpecificationGenerator(
        cerbos: CerbosBlockingClient,
        principalRepository: MockPrincipalRepository
    ): ResourceSpecificationGenerator = ResourceSpecificationGenerator(
        cerbos, principalRepository, mapOf(
            "request.resource.attr.aBool" to Boolean::class.java,
            "request.resource.attr.aString" to String::class.java
        )
    )
}