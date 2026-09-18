package dev.cerbos.queryplan.exposed

import org.testcontainers.containers.GenericContainer
import java.nio.file.Files
import java.nio.file.Path

/**
 * Pinned Cerbos image used by [AdversarialConformanceTest], the one suite here that starts a PDP.
 * Every other suite in this adapter runs offline.
 *
 * The PDP is the oracle for BOTH sides of the differential — it produces the plan under test and
 * the per-row `check()` decisions it is compared against — so which build answered is the one fact
 * a green run cannot be read without. [assertPinned] is where that fact is asserted rather than
 * printed: the container's resolved digest must be the one `conformance/CERBOS_IMAGE_DIGEST` names,
 * unless the `cerbos.test.image` override is set, in which case the run says so in a way nobody can
 * miss.
 *
 * **Why pinned.** The suite pins planner-shape-dependent behaviour — the fail-closed messages
 * `conformance/actions.json` declares, the `has()` over-grant tripwire, the differential oracle's
 * row sets. A floating `:latest` tag makes past green runs unreproducible once the tag moves and
 * lets upstream planner changes flow into CI unnoticed.
 *
 * **Bump policy.** The pin lives in `conformance/CERBOS_VERSION` and
 * `conformance/CERBOS_IMAGE_DIGEST`, and bumping it is a deliberate, reviewed change: update both
 * halves, run the full suite, and re-evaluate every upstream-tracking test — in particular
 * [AdversarialConformanceTest.upstreamHasFoldOverGrantTripwire], whose "fires when upstream fixes
 * the fold" property is dormant between bumps and only re-checks upstream behaviour when the pinned
 * image moves. Override per run with `-Dcerbos.test.image=<image reference>` to trial a newer PDP
 * without editing source. (Spelled with a placeholder rather than a real reference on purpose:
 * validate-corpus.sh scans the whole repository for Cerbos image references and holds every one of
 * them to the corpus pin, including ones that only appear in documentation.)
 */
internal object CerbosTestImage {

    /** The system property that swaps the oracle. Never set by any workflow. */
    const val OVERRIDE_PROPERTY: String = "cerbos.test.image"

    /** True when a caller replaced the pinned oracle for this run. */
    val OVERRIDDEN: Boolean = System.getProperty(OVERRIDE_PROPERTY) != null

    val IMAGE: String = System.getProperty(OVERRIDE_PROPERTY) ?: defaultImage()

    private fun conformanceDir(): Path = Corpus.conformanceDir()

    /** The digest half of the pin, exactly as `conformance/CERBOS_IMAGE_DIGEST` spells it. */
    fun pinnedDigest(): String =
        Files.readString(conformanceDir().resolve("CERBOS_IMAGE_DIGEST")).trim()

    // Tag AND digest: the tag records which release this is, the digest makes the pin immune to the
    // tag being re-pointed. validate-corpus.sh asserts the two agree everywhere they are restated.
    private fun defaultImage(): String {
        val tag = Files.readString(conformanceDir().resolve("CERBOS_VERSION")).trim()
        return "ghcr.io/cerbos/cerbos:$tag@${pinnedDigest()}"
    }

    /** Every repo digest Docker records for the image a started container runs. */
    fun resolvedDigests(container: GenericContainer<*>): List<String> =
        container.dockerClient.inspectImageCmd(container.dockerImageName).exec().repoDigests.orEmpty()

    /**
     * The started container runs the pinned build, or the run is loudly NOT the pinned run.
     *
     * Without this the override property changes the oracle silently: the resolved digest was
     * printed, and a log line is not an assertion. A wrong digest here is a Docker cache holding a
     * different build under the pinned reference, or a pin whose two halves disagree — either way
     * the differential would be against a PDP nobody chose, so it fails rather than runs.
     */
    fun assertPinned(container: GenericContainer<*>) {
        val digests = resolvedDigests(container)
        if (OVERRIDDEN) {
            val banner = "=".repeat(78)
            println(
                """
                |$banner
                |==> CERBOS PDP OVERRIDDEN via -D$OVERRIDE_PROPERTY=$IMAGE
                |==> This run is NOT against the pinned oracle
                |    (conformance/CERBOS_VERSION @ CERBOS_IMAGE_DIGEST).
                |==> Resolved digests: $digests
                |==> Its result proves nothing about the pinned PDP.
                |$banner
                """.trimMargin(),
            )
            return
        }
        val pinned = pinnedDigest()
        if (digests.none { it.endsWith("@$pinned") }) {
            throw AssertionError(
                "the started Cerbos container does not run the pinned build:" +
                    " conformance/CERBOS_IMAGE_DIGEST is $pinned but Docker resolved $IMAGE to" +
                    " $digests",
            )
        }
        println("==> Adversarial-oracle Cerbos PDP: $IMAGE (digest $pinned)")
    }
}
