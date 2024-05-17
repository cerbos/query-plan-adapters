package dev.cerbos.springdataspecificationadapter

import dev.cerbos.sdk.CerbosBlockingClient

class ResourceSpecificationGenerator(
    cerbos: CerbosBlockingClient,
    principalRepository: PrincipalRepository,
    policyPathToType: Map<String, Class<*>>
) : BaseCerbosAuthzSpecificationGenerator<Resource>(cerbos, principalRepository, policyPathToType)
