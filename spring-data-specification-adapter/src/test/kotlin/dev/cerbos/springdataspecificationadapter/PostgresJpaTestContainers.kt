package dev.cerbos.springdataspecificationadapter

import org.junit.jupiter.api.BeforeAll
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container

open class PostgresJpaTestcontainers {

    companion object {

        @Container
        val database: PostgreSQLContainer<*> = PostgreSQLContainer("postgres:15.2").apply {
            this.withPassword("postgres").withUsername("postgres")
        }

        @DynamicPropertySource
        @JvmStatic
        fun registerDynamicProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url", database::getJdbcUrl)
            registry.add("spring.datasource.username", database::getUsername)
            registry.add("spring.datasource.password", database::getPassword)
            registry.add("spring.datasource.driver-class-name", database::getDriverClassName)
            registry.add("spring.jpa.databasePlatform") { "org.hibernate.dialect.PostgreSQLDialect" }
        }

        @BeforeAll
        @JvmStatic
        fun start() {
            database.start()
        }

    }

}
