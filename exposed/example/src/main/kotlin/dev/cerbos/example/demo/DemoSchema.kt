package dev.cerbos.example.demo

import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.dao.id.EntityID
import org.jetbrains.exposed.v1.core.dao.id.IdTable
import org.jetbrains.exposed.v1.dao.Entity
import org.jetbrains.exposed.v1.dao.EntityClass

/**
 * The demo domain's one table: four flat scalar columns and a string primary key, matching
 * `demo/seeds.json` exactly. No relations and no nullable columns — the shapes that split the
 * adapters are the conformance corpus's job, and this directory proves plumbing
 * (`docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md`).
 *
 * An [IdTable] rather than a plain `Table` so the same schema serves both halves of usage shape 1:
 * the DSL query, and the DAO `DocumentEntity.find { }` the adapter's `Op<Boolean>` is handed to
 * unchanged.
 *
 * `is_public` is not a typo for the attribute name. The policy calls the attribute
 * `request.resource.attr.public` and this column is `isPublic`/`is_public`; a mapping exists
 * precisely so the two are allowed to differ, and an example that named them identically would
 * demonstrate nothing. `region` and `archived` are the APPLICATION's columns: no rule in
 * `demo/policies/document.yaml` mentions either, and composing them with the adapter's filter is
 * usage shape 5.
 */
object Documents : IdTable<String>("documents") {
    override val id: Column<EntityID<String>> = varchar("id", 32).entityId()
    val ownerId = varchar("owner_id", 64)
    val isPublic = bool("is_public")
    val region = varchar("region", 32)
    val archived = bool("archived")

    override val primaryKey = PrimaryKey(id)
}

/**
 * The DAO view of the same table.
 *
 * It exists for one assertion: that the `Op<Boolean>` the adapter returns goes into
 * `EntityClass.find { }` as readily as into `Query.where { }`. That is a claim about the adapter's
 * return TYPE — an ordinary Exposed predicate, not a query the adapter built — and the DAO is the
 * one caller that would expose it as false.
 */
class DocumentEntity(id: EntityID<String>) : Entity<String>(id) {
    companion object : EntityClass<String, DocumentEntity>(Documents)

    var ownerId by Documents.ownerId
    var isPublic by Documents.isPublic
    var region by Documents.region
    var archived by Documents.archived
}
