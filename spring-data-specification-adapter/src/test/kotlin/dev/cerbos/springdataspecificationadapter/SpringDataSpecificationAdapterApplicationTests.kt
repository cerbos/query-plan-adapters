package dev.cerbos.springdataspecificationadapter


import org.junit.ClassRule
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.data.jpa.domain.Specification
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer

@SpringBootTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(TestConfig::class)
class SpringDataSpecificationAdapterApplicationTests : PostgresJpaTestcontainers() {

    companion object {

        @JvmField
        val cerbos = GenericContainer("ghcr.io/cerbos/cerbos:dev")
            .withExposedPorts(3592)
            .setCommand("server")

    }


    @Autowired
    private lateinit var resourceRepository: ResourceRepository

    @Autowired
    private lateinit var resourceSpecificationGenerator: ResourceSpecificationGenerator

    @Suppress("RemoveRedundantBackticks")
    @Test
    fun `equals`() {

        val specification: Specification<Resource> = resourceSpecificationGenerator.specificationFor(
            id = "principal", resource = "resource", action = "equal"

        )
        resourceRepository.findAll(specification)

    }

    @Suppress("RemoveRedundantBackticks")
    @Test
    fun `ne`() {

        val specification: Specification<Resource> = resourceSpecificationGenerator.specificationFor(
            id = "principal", resource = "resource", action = "ne"

        )
        resourceRepository.findAll(specification)

    }

    @Suppress("RemoveRedundantBackticks")
    @Test
    fun `and`() {

        val specification: Specification<Resource> = resourceSpecificationGenerator.specificationFor(
            id = "principal", resource = "resource", action = "and"

        )
        resourceRepository.findAll(specification)

    }

    @Suppress("RemoveRedundantBackticks")
    @Test
    fun `or`() {

        val specification: Specification<Resource> = resourceSpecificationGenerator.specificationFor(
            id = "principal", resource = "resource", action = "or"

        )
        resourceRepository.findAll(specification)

    }

    @Suppress("RemoveRedundantBackticks")
    @Test
    fun `nand`() {

        val specification: Specification<Resource> = resourceSpecificationGenerator.specificationFor(
            id = "principal", resource = "resource", action = "nand"

        )
        resourceRepository.findAll(specification)

    }

    @Suppress("RemoveRedundantBackticks")
    @Test
    fun `nor`() {

        val specification: Specification<Resource> = resourceSpecificationGenerator.specificationFor(
            id = "principal", resource = "resource", action = "nor"

        )
        resourceRepository.findAll(specification)

    }

    @Suppress("RemoveRedundantBackticks")
    @Test
    fun `equal-nested`() {

        val specification: Specification<Resource> = resourceSpecificationGenerator.specificationFor(
            id = "principal", resource = "resource", action = "equal-nested"

        )
        resourceRepository.findAll(specification)

    }

    @Suppress("RemoveRedundantBackticks")
    @Test
    fun `relation-is-not`() {

        val specification: Specification<Resource> = resourceSpecificationGenerator.specificationFor(
            id = "principal", resource = "resource", action = "relation-is-not"

        )
        resourceRepository.findAll(specification)

    }


}
