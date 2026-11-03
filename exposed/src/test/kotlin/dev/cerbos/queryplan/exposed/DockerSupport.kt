package dev.cerbos.queryplan.exposed

import org.testcontainers.DockerClientFactory

/**
 * Whether a Docker daemon is reachable, for the suites that may SKIP without one.
 *
 * Which suites those are is a deliberate split, not a convenience. A suite that starts a container
 * to answer a question about the ENVIRONMENT — whether the offline renderer's stub connections
 * still match real servers, whether the pinned PDP still ships the wire shape a unit test
 * hand-builds — has nothing to assert when there is no environment to ask, so it skips and says so.
 *
 * `AdversarialConformanceTest` deliberately does NOT use this. It is the oracle: a harness that
 * silently skips is a harness that passes vacuously, which is the failure mode the whole corpus
 * exists to prevent, so it fails loudly without Docker instead.
 */
internal fun dockerAvailable(): Boolean =
    runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
