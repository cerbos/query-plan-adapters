package dev.cerbos.queryplan.exposed

import com.google.protobuf.util.JsonFormat
import dev.cerbos.sdk.CerbosBlockingClient
import dev.cerbos.sdk.CerbosClientBuilder
import dev.cerbos.sdk.builders.AttributeValue
import dev.cerbos.sdk.builders.Principal
import dev.cerbos.sdk.builders.Resource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable
import java.nio.file.Files
import java.nio.file.Path

/**
 * What the PINNED PDP actually ships for the shapes this review hand-builds.
 *
 * Every other review suite states the CEL its shape comes from and builds the operands by hand,
 * because `conformance/` carries no fixture for any of them — finding those gaps is the point of
 * the review. This suite is what keeps those hand-built plans honest without editing the corpus: it
 * loads a policy of its OWN into the pinned PDP and asserts the planner's wire output is the shape
 * the review assumed. If the planner starts folding one of them away, the finding that rests on it
 * fails here rather than quietly becoming a claim about a plan nobody can produce.
 *
 * It is NOT a substitute for a corpus action, and none of these belongs here permanently: each one
 * is a shape `conformance/policies/adversarial.yaml` should carry, asked of every adapter. The
 * recorded plans are written to `build/reports/review-planner.txt` for the port.
 */
@Tag("docker")
class ReviewPlannerShapeTest {

    @Test
    fun `an unsolvable concatenation ships as not(eq(add(variable, value), value))`() {
        // The plan REVIEW FINDING 4 is built on, in both polarities.
        assertEquals(
            "{\"expression\":{\"operator\":\"not\",\"operands\":[{\"expression\":{\"operator\":\"eq\"," +
                "\"operands\":[{\"expression\":{\"operator\":\"add\",\"operands\":[" +
                "{\"variable\":\"request.resource.attr.aOptionalString\"},{\"value\":\"!\"}]}}," +
                "{\"value\":\"nope\"}]}}]}}",
            plan("not-unsolvable-concat"),
        )
        assertTrue(plan("not-unsolvable-concat-ne").contains("\"operator\":\"ne\""), plan("not-unsolvable-concat-ne"))
    }

    @Test
    fun `a hierarchy overlap over a list carrying a column ships unfolded`() {
        // The plan REVIEW FINDING 5 is built on: the planner keeps both `hierarchy()` calls and the
        // `list()` construction, so the adapter really does decide the overlap itself.
        val shipped = plan("overlaps-list-prefix")
        assertTrue(shipped.contains("\"operator\":\"overlaps\""), shipped)
        assertTrue(shipped.contains("\"operator\":\"list\""), shipped)
        assertTrue(shipped.contains("request.resource.attr.scope"), shipped)
    }

    @Test
    fun `an empty intersection ships unfolded while an empty in does not`() {
        // Why REVIEW FINDING 6 is about `hasIntersection` and not about `in`: the planner folds
        // `x in []` to KIND_ALWAYS_ALLOWED under a negation — so MembershipTranslator's
        // `scalarIsAnyOf` empty branch is unreachable from a policy — and ships
        // `not(hasIntersection(variable, []))` verbatim.
        assertEquals("<always-allowed>", plan("not-in-empty"))
        val intersection = plan("not-hasint-empty")
        assertTrue(intersection.contains("\"operator\":\"hasIntersection\""), intersection)
        assertTrue(intersection.contains("\"value\":[]"), intersection)
    }

    @Test
    fun `a text attribute compared with a numeric principal attribute ships as a plain equality`() {
        // The plan REVIEW FINDING 1 is built on: the principal attribute folds to a NUMBER and the
        // comparison arrives as an ordinary leaf, with nothing in it that names a type.
        assertEquals(
            "{\"expression\":{\"operator\":\"eq\",\"operands\":[" +
                "{\"variable\":\"request.resource.attr.aString\"},{\"value\":3.0}]}}",
            plan("text-eq-number"),
        )
    }

    @Test
    fun `attribute-in-attribute ships member-first and verbatim`() {
        // The plan REVIEW FINDINGS 2 and 3 are built on — identical to the corpus's `in-var-var`
        // fixture apart from the attribute name, which is what lets those findings vary only the
        // caller-supplied convention.
        assertEquals(
            "{\"expression\":{\"operator\":\"in\",\"operands\":[" +
                "{\"variable\":\"request.resource.attr.aOptionalString\"}," +
                "{\"variable\":\"request.resource.attr.tagNames\"}]}}",
            plan("in-var-var-undeclared"),
        )
    }

    companion object {
        private var container: GenericContainer<*>? = null
        private var client: CerbosBlockingClient? = null
        private val recorded = LinkedHashMap<String, String>()

        @BeforeAll
        @JvmStatic
        fun setUp() {
            val started = GenericContainer(CerbosTestImage.IMAGE)
                .withExposedPorts(3593)
                .withCommand("server", "--set=storage.disk.directory=/policies")
                .withEnv("CERBOS_NO_TELEMETRY", "1")
                .withLogConsumer(Slf4jLogConsumer(LoggerFactory.getLogger("cerbos-review-pdp")))
                .waitingFor(Wait.forLogMessage(".*Starting gRPC server.*", 1))
                .withCopyToContainer(Transferable.of(POLICY.toByteArray()), "/policies/review.yaml")
            started.start()
            container = started
            CerbosTestImage.assertPinned(started)
            client = CerbosClientBuilder("${started.host}:${started.getMappedPort(3593)}")
                .withPlaintext().buildBlockingClient()
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            runCatching {
                val target = Path.of(System.getProperty("user.dir"), "build", "reports", "review-planner.txt")
                Files.createDirectories(target.parent)
                Files.writeString(
                    target,
                    recorded.entries.joinToString("\n\n") { "### ${it.key}\n  ${it.value}" } + "\n",
                )
            }
            container?.stop()
        }

        private val PRINTER = JsonFormat.printer().omittingInsignificantWhitespace()

        /** The planner's condition for [action], as canonical JSON, or a marker for a folded plan. */
        fun plan(action: String): String = recorded.getOrPut(action) {
            val result = client!!.plan(
                Principal.newInstance("u1", "USER")
                    .withAttribute("level", AttributeValue.doubleValue(3.0))
                    .withAttribute("empty", AttributeValue.listValue(emptyList<AttributeValue>())),
                Resource.newInstance("reviewres"),
                listOf(action),
            )
            when {
                result.isAlwaysAllowed -> "<always-allowed>"
                result.isAlwaysDenied -> "<always-denied>"
                else -> PRINTER.print(result.condition.orElseThrow())
            }
        }

        val POLICY: String = """
            apiVersion: api.cerbos.dev/v1
            resourcePolicy:
              version: default
              resource: reviewres
              rules:
                - actions: ["not-unsolvable-concat"]
                  effect: EFFECT_ALLOW
                  roles: ["USER"]
                  condition:
                    match:
                      expr: '!((R.attr.aOptionalString + "!") == "nope")'
                - actions: ["not-unsolvable-concat-ne"]
                  effect: EFFECT_ALLOW
                  roles: ["USER"]
                  condition:
                    match:
                      expr: '!((R.attr.aOptionalString + "!") != "nope")'
                - actions: ["overlaps-list-prefix"]
                  effect: EFFECT_ALLOW
                  roles: ["USER"]
                  condition:
                    match:
                      expr: 'hierarchy("projects", ":").overlaps(hierarchy(["projects", R.attr.scope]))'
                - actions: ["not-in-empty"]
                  effect: EFFECT_ALLOW
                  roles: ["USER"]
                  condition:
                    match:
                      expr: '!(R.attr.aOptionalString in P.attr.empty)'
                - actions: ["not-hasint-empty"]
                  effect: EFFECT_ALLOW
                  roles: ["USER"]
                  condition:
                    match:
                      expr: '!hasIntersection(R.attr.tagNames, P.attr.empty)'
                - actions: ["text-eq-number"]
                  effect: EFFECT_ALLOW
                  roles: ["USER"]
                  condition:
                    match:
                      expr: 'R.attr.aString == P.attr.level'
                - actions: ["in-var-var-undeclared"]
                  effect: EFFECT_ALLOW
                  roles: ["USER"]
                  condition:
                    match:
                      expr: 'R.attr.aOptionalString in R.attr.tagNames'
        """.trimIndent()
    }
}
