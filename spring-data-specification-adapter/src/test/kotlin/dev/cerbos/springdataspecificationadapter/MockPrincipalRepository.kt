package dev.cerbos.springdataspecificationadapter

import dev.cerbos.sdk.builders.Principal

class MockPrincipalRepository : PrincipalRepository {
    override fun retrievePrincipalById(id: String): Principal? {
        return Principal.newInstance(id).withRoles("USER")
    }
}