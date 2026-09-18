package dev.cerbos.example.demo

import dev.cerbos.queryplan.exposed.ExposedQueryPlanAdapter
import dev.cerbos.queryplan.exposed.Options
import dev.cerbos.queryplan.exposed.QueryPlanFilter
import dev.cerbos.queryplan.exposed.cerbosMapping
import dev.cerbos.sdk.CerbosBlockingClient
import dev.cerbos.sdk.PlanResourcesResult
import dev.cerbos.sdk.builders.Principal
import dev.cerbos.sdk.builders.Resource
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.selectAll

/**
 * The five usage shapes of the shared demo domain, against a real Exposed `Query` and a real
 * store.
 *
 * This is not a test of what the adapter translates — `../src/test/kotlin/.../` proves that
 * against a hostile corpus with a live PDP as the oracle, on four stores. It proves the two things
 * that harness structurally cannot:
 *
 * 1. **Packaging.** The adapter imports above resolve through its real Maven coordinate rather
 *    than through its source set, so its POM and Gradle module metadata are executed — dependency
 *    scopes included, and Exposed's absence from them most of all. See
 *    `docs/adr/0002-examples-install-the-packed-artifact.md`.
 * 2. **Usage shape.** A harness runs one flat filtered query. Consumers also paginate, and compose
 *    the adapter's predicate with predicates of their own. Shape 5 below is the one that earns the
 *    exercise.
 *
 * The plan kind is reported alongside the ids because `demo/expected.json` pins it: that is what
 * stops this program returning all the rows for `admin-view` without ever having reached the PDP.
 * It is read off the adapter's own [QueryPlanFilter] rather than off the SDK's plan predicates,
 * because the [QueryPlanFilter] variant is what the rest of this class branches on — deriving it
 * from the plan instead would leave the adapter free to classify a plan one way and this program
 * to report it another.
 */
class DemoShapes(private val cerbos: CerbosBlockingClient, private val seeds: DemoSeeds) {

    /** One shape's answer, as `demo/expected.json` spells it. */
    data class ShapeResult(val kind: String, val ids: List<String>)

    /** Shape 4 additionally reports the page size it asked for and the size of each page read. */
    data class PaginatedShapeResult(
        val kind: String,
        val ids: List<String>,
        val pageSize: Int,
        val pageSizes: List<Int>,
    )

    /** Seeds the store, runs every shape, and returns the `shapes` object to emit. */
    fun run(): Map<String, Any> {
        seed()

        return linkedMapOf(
            "filtered" to linkedMapOf(
                "alice/view" to filtered("alice", "view"),
                "bob/view" to filtered("bob", "view"),
            ),
            "alwaysAllowed" to linkedMapOf(
                "admin/admin-view" to filtered("admin", "admin-view"),
            ),
            "alwaysDenied" to linkedMapOf(
                "alice/publish" to filtered("alice", "publish"),
            ),
            "paginated" to linkedMapOf(
                "alice/view" to paginated("alice", "view", pageSize = 2),
                "admin/admin-view" to paginated("admin", "admin-view", pageSize = 3),
            ),
            "composed" to linkedMapOf(
                "alice/view" to composed("alice", "view"),
                "bob/view" to composed("bob", "view"),
                "admin/admin-view" to composed("admin", "admin-view"),
                "alice/publish" to composed("alice", "publish"),
            ),
        )
    }

    // -- the five usage shapes --

    /**
     * Shapes 1, 2 and 3: a plain filtered list — the adapter's predicate is the whole query.
     *
     * Every kind runs its query, including `ALWAYS_DENIED`. Skipping the store on
     * `QueryPlanFilter.AlwaysDenied` is a supported optimisation and the reason that variant is
     * distinct from a bare `Op.FALSE`, but executing the query is what demonstrates the property
     * shape 5 exists to check, so this method takes the uniform path.
     *
     * The DAO cross-check is an EXTRA beyond the shared floor, which ADR 0001 explicitly allows.
     * It asserts rather than reports: `demo/expected.json` is diffed exactly, so a sixth key would
     * fail the runner. What it proves is that the adapter returns an ordinary Exposed predicate
     * — the same value satisfies `Query.where { }` and `EntityClass.find { }` — which no suite
     * under `../src/test` asks, because none of them builds a DAO entity.
     */
    private fun filtered(principalId: String, action: String): ShapeResult {
        val filter = authorize(principalId, action)

        val ids = Documents.selectAll()
            .where { filter.toOp() }
            .map { it[Documents.id].value }
            .sorted()

        val daoIds = DocumentEntity.find { filter.toOp() }.map { it.id.value }.sorted()
        check(daoIds == ids) {
            "the same predicate returned $ids through the DSL and $daoIds through the DAO"
        }

        return ShapeResult(kind(filter), ids)
    }

    /**
     * Shape 4: pagination applied on top of the filter, with Exposed's own `orderBy`, `limit` and
     * `offset` composed onto the query the adapter's predicate already filters.
     *
     * Reported as page SIZES plus the sorted union of the ids, never as per-page order:
     * `demo/expected.json` is shared by every example and several of the stores behind it have no
     * total order to paginate by. The `orderBy` is still required for the paging itself to be
     * correct — without a total order, successive `offset`s may repeat or omit rows — which is a
     * separate concern from how the result is asserted.
     */
    private fun paginated(principalId: String, action: String, pageSize: Int): PaginatedShapeResult {
        val filter = authorize(principalId, action)

        val pageSizes = mutableListOf<Int>()
        val ids = mutableListOf<String>()
        var offset = 0L
        while (true) {
            val page = Documents.selectAll()
                .where { filter.toOp() }
                .orderBy(Documents.id, SortOrder.ASC)
                .limit(pageSize)
                .offset(offset)
                .map { it[Documents.id].value }
            if (page.isEmpty()) break
            pageSizes += page.size
            ids += page
            // Reading until a page comes back short stops the loop on a total that is an exact
            // multiple of the page size as well as on one that is not.
            if (page.size < pageSize) break
            offset += pageSize
        }

        return PaginatedShapeResult(kind(filter), ids.sorted(), pageSize, pageSizes)
    }

    /**
     * Shape 5: the adapter's predicate ANDed with the application's own.
     *
     * All three plan kinds go through here on purpose. An `ALWAYS_ALLOWED` plan has no predicate
     * to AND with — [QueryPlanFilter.toOp] gives `Op.TRUE`, a true identity for `and` — and an
     * `ALWAYS_DENIED` one must not have its denial undone. Those are the two cases that break
     * first, which is why `demo/expected.json` pins all three here.
     */
    private fun composed(principalId: String, action: String): ShapeResult {
        val filter = authorize(principalId, action)
        val ids = Documents.selectAll()
            .where { filter.toOp() and applicationFilter() }
            .map { it[Documents.id].value }
            .sorted()
        return ShapeResult(kind(filter), ids)
    }

    /**
     * The APPLICATION's own predicate — `archived == false AND region == 'emea'` — read from
     * `demo/seeds.json` rather than written out here, so it cannot drift from the expectations
     * `demo/scripts/validate-demo.sh` recomputes from the same field.
     *
     * Written with Exposed's ordinary `eq` sugar, which the ADAPTER deliberately never uses for a
     * plan-derived value: `eq` binds a constant through the column's type and rewrites `null` into
     * `IS NULL`, and a translator cannot afford either. An application comparing its own column to
     * its own constant is the case that sugar is for, and the asymmetry is the shape of the
     * composition a consumer actually writes.
     */
    private fun applicationFilter(): Op<Boolean> {
        val filter = seeds.applicationFilter
        return (Documents.archived eq filter.archived) and (Documents.region eq filter.region)
    }

    // -- plumbing --

    /**
     * The plan for one principal and one action, translated.
     *
     * The action is passed as a single-element list because `plan(Principal, Resource, String)` is
     * deprecated in cerbos-sdk-java — a detail an example is exactly the wrong place to hide,
     * since it is the code a consumer copies.
     */
    private fun authorize(principalId: String, action: String): QueryPlanFilter {
        val subject = seeds.principal(principalId)
        val plan: PlanResourcesResult = cerbos.plan(
            Principal.newInstance(subject.id, *subject.roles.toTypedArray()),
            Resource.newInstance(RESOURCE_KIND),
            listOf(action),
        )
        return ExposedQueryPlanAdapter.toFilter(plan, Options.of(DOCUMENT_ATTRIBUTES))
    }

    /**
     * The plan kind as `demo/expected.json` spells it. Exhaustive over the sealed result, so a
     * fourth variant would be a compile error here rather than a diff in the shared runner.
     */
    private fun kind(filter: QueryPlanFilter): String = when (filter) {
        is QueryPlanFilter.AlwaysAllowed -> "KIND_ALWAYS_ALLOWED"
        is QueryPlanFilter.AlwaysDenied -> "KIND_ALWAYS_DENIED"
        is QueryPlanFilter.Conditional -> "KIND_CONDITIONAL"
    }

    private fun seed() {
        SchemaUtils.create(Documents)
        seeds.documents.forEach { document ->
            Documents.insert {
                it[id] = document.id
                it[ownerId] = document.ownerId
                it[isPublic] = document.isPublic
                it[region] = document.region
                it[archived] = document.archived
            }
        }
        System.err.println("seeded ${seeds.documents.size} documents into H2")
    }

    private companion object {
        const val RESOURCE_KIND = "document"

        /**
         * Cerbos attribute names are not column names, so a consumer always writes one of these.
         * Both entries are load-bearing: an unmapped attribute makes the adapter throw
         * `UnmappedAttributeException` rather than guess a column, and `public` resolves to
         * `Documents.isPublic` because the policy's name for the attribute and the table's name
         * for the column are allowed to differ — which is the point of having a mapping at all.
         *
         * `region` and `archived` are deliberately absent: no rule in
         * `demo/policies/document.yaml` names them, and they exist so the application can own a
         * predicate the policy never sees.
         */
        val DOCUMENT_ATTRIBUTES = cerbosMapping {
            "request.resource.attr.ownerId" to Documents.ownerId
            "request.resource.attr.public" to Documents.isPublic
        }
    }
}
