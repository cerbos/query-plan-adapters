package dev.cerbos.springdataspecificationadapter

import dev.cerbos.sdk.builders.Principal

fun interface PrincipalRepository {

    fun retrievePrincipalById(id: String): Principal?
}
