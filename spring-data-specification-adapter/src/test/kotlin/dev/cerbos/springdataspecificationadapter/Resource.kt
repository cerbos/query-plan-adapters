package dev.cerbos.springdataspecificationadapter

import jakarta.persistence.Entity
import jakarta.persistence.Id
import java.util.UUID

@Entity
data class Resource(
    @Id val id: UUID,
    val aBool: Boolean,
    val aString: String,
    val aNumber: Number
)
