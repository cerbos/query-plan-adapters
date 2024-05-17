package dev.cerbos.springdataspecificationadapter

import org.springframework.data.jpa.repository.JpaSpecificationExecutor
import org.springframework.data.repository.CrudRepository
import org.springframework.stereotype.Repository
import java.util.UUID

@Repository
interface ResourceRepository : CrudRepository<Resource, UUID>, JpaSpecificationExecutor<Resource>
