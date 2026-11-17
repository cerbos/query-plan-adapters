package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.AttributeMapping.Relation.Cardinality
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.ForeignKeyConstraint
import org.jetbrains.exposed.v1.core.Table

internal class InferredKeys(val from: Column<*>, val to: Column<*>, val cardinality: Cardinality) {
    fun describe(): String =
        "${to.table.tableName}.${to.name} = ${from.table.tableName}.${from.name} ($cardinality)"
}

internal fun inferRelationKeys(scope: Table?, table: Table): InferredKeys {
    val parent = requireScope(scope, table.tableName)
    val keys = table.foreignKeys.filter { it.targetTable === parent }.map { pointsAtScope(it) } +
        parent.foreignKeys.filter { it.targetTable === table }.map { pointsAway(it) }
    val single = keys.filterNotNull()
    require(keys.size == single.size) {
        "A foreign key between ${parent.tableName} and ${table.tableName} is composite: declare the relation with one(...) or many(...)"
    }
    require(single.isNotEmpty()) {
        "No foreign key links ${parent.tableName} and ${table.tableName}: declare the relation with one(...) or many(...)"
    }
    require(single.size == 1) {
        "Several foreign keys link ${parent.tableName} and ${table.tableName} (${single.joinToString { it.describe() }}): " +
            "name one with relation(via = ...), or declare the relation with one(...) or many(...)"
    }
    return single.single()
}

internal fun inferRelationKeys(scope: Table?, via: Column<*>): InferredKeys {
    val parent = requireScope(scope, "via = ${via.table.tableName}.${via.name}")
    val fk = requireNotNull(via.foreignKey) { "${via.table.tableName}.${via.name} references no column" }
    val keys = when {
        via.table === parent -> pointsAway(fk)
        fk.targetTable === parent -> pointsAtScope(fk)
        else -> throw IllegalArgumentException(
            "${via.table.tableName}.${via.name} neither belongs to nor references ${parent.tableName}",
        )
    }
    return requireNotNull(keys) {
        "The foreign key on ${via.table.tableName}.${via.name} is composite: declare the relation with one(...) or many(...)"
    }
}

private fun requireScope(scope: Table?, what: String): Table = requireNotNull(scope) {
    "relation($what) at the top level needs the root table: build the mapping with cerbosMapping(root = ...)"
}

private fun pointsAtScope(fk: ForeignKeyConstraint): InferredKeys? {
    val (child, key) = fk.references.entries.singleOrNull()?.let { registered(it.key) to registered(it.value) } ?: return null
    return InferredKeys(from = key, to = child, cardinality = cardinalityOf(child))
}

private fun pointsAway(fk: ForeignKeyConstraint): InferredKeys? {
    val (own, target) = fk.references.entries.singleOrNull()?.let { registered(it.key) to registered(it.value) } ?: return null
    return InferredKeys(from = own, to = target, cardinality = cardinalityOf(target))
}

// `nullable()` replaces a column after `references(...)` captured it; read the table's own copy.
private fun registered(column: Column<*>): Column<*> =
    column.table.columns.firstOrNull { it.name == column.name } ?: column

private fun cardinalityOf(column: Column<*>): Cardinality =
    if (isProvablyUnique(column)) Cardinality.ONE else Cardinality.MANY

private fun isProvablyUnique(column: Column<*>): Boolean {
    val table = column.table
    if (table.primaryKey?.columns?.singleOrNull()?.name == column.name) return true
    return table.indices.any {
        it.unique && it.filterCondition == null && it.functions.isNullOrEmpty() && it.columns.singleOrNull()?.name == column.name
    }
}
