package dev.cerbos.queryplan.exposed

import com.google.protobuf.NullValue
import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * Kind 2: the corpus maps no boolean and no to-one field under the EXPLICIT convention, and each
 * harness has one mapping. An explicit null is a value to `==` and `!=` only: in condition position
 * it is a CEL error, and a to-one row that is absent is a missing path, not a null.
 *
 * | id | flag  | parent               |
 * |----|-------|----------------------|
 * | d1 | true  | name "x", flag true  |
 * | d2 | NULL  | none                 |
 * | d3 | false | name NULL, flag NULL |
 */
class ExplicitNullValueTest {

    @Test
    fun `a negated bare boolean denies the row whose explicit null it reads`() {
        assertEquals(listOf("d3"), ids(not(variable(FLAG)), EXPLICIT))
    }

    @Test
    fun `a negated bare boolean through a to-one hop denies a null and a missing parent`() {
        assertEquals(emptyList<String>(), ids(not(variable(PARENT_FLAG)), EXPLICIT))
    }

    @Test
    fun `a comparison through a missing to-one parent denies under both polarities`() {
        val equal = expression("eq", variable(PARENT_NAME), value("x"))
        assertEquals(listOf("d1"), ids(equal, EXPLICIT))
        assertEquals(listOf("d3"), ids(not(equal), EXPLICIT))
        assertEquals(listOf("d3"), ids(expression("ne", variable(PARENT_NAME), value("x")), EXPLICIT))
    }

    @Test
    fun `a null literal matches a null column of a parent, never a missing parent`() {
        val isNull = expression("eq", variable(PARENT_NAME), NULL)
        assertEquals(listOf("d3"), ids(isNull, EXPLICIT))
        assertEquals(listOf("d1"), ids(not(isNull), EXPLICIT))
        val callLevel = Options.of(UNDECLARED).withNullAttributeRepresentation(NullAttributeRepresentation.EXPLICIT)
        assertEquals(listOf("d3"), ids(isNull, callLevel))
    }

    @Test
    fun `two columns compared through a missing parent deny`() {
        val same = expression("eq", variable(PARENT_FLAG), variable(FLAG))
        assertEquals(listOf("d1"), ids(same, EXPLICIT))
        assertEquals(listOf("d3"), ids(not(same), EXPLICIT))
    }

    companion object {
        private const val FLAG = "request.resource.attr.flag"
        private const val PARENT_NAME = "request.resource.attr.parent.name"
        private const val PARENT_FLAG = "request.resource.attr.parent.flag"

        object Docs : Table("explicit_docs") {
            val id = varchar("id", 32)
            val flag = bool("flag").nullable()
            override val primaryKey = PrimaryKey(id)
        }

        object DocParents : Table("explicit_parents") {
            val docId = varchar("doc_id", 32).uniqueIndex()
            val name = varchar("name", 64).nullable()
            val flag = bool("flag").nullable()
        }

        private fun mapping(nulls: NullAttributeRepresentation?): AttributeMappings = cerbosMapping {
            FLAG to field(Docs.flag, nulls)
            "request.resource.attr.parent" to one(DocParents, from = Docs.id, to = DocParents.docId) {
                "name" to field(DocParents.name, nulls)
                "flag" to field(DocParents.flag, nulls)
            }
        }

        private val UNDECLARED = mapping(null)
        private val EXPLICIT = Options.of(mapping(NullAttributeRepresentation.EXPLICIT))

        private val NULL: Operand =
            Operand.newBuilder().setValue(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)).build()

        private val database by lazy {
            val db = Database.connect("jdbc:h2:mem:explicit_values;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
            transaction(db) {
                SchemaUtils.create(Docs, DocParents)
                listOf("d1" to true, "d2" to null, "d3" to false).forEach { (docId, value) ->
                    Docs.insert {
                        it[id] = docId
                        it[flag] = value
                    }
                }
                DocParents.insert {
                    it[docId] = "d1"
                    it[name] = "x"
                    it[flag] = true
                }
                DocParents.insert {
                    it[docId] = "d3"
                    it[name] = null
                    it[flag] = null
                }
            }
            db
        }

        private fun ids(condition: Operand, options: Options): List<String> {
            val plan = PlanResourcesFilter.newBuilder()
                .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                .setCondition(condition)
                .build()
            val op = ExposedQueryPlanAdapter.toFilter(plan, options).toOp()
            return transaction(database) { Docs.selectAll().where(op).map { it[Docs.id] }.sorted() }
        }

        private fun variable(name: String): Operand = Operand.newBuilder().setVariable(name).build()

        private fun value(text: String): Operand =
            Operand.newBuilder().setValue(Value.newBuilder().setStringValue(text)).build()

        private fun expression(operator: String, vararg operands: Operand): Operand = Operand.newBuilder()
            .setExpression(Expression.newBuilder().setOperator(operator).addAllOperands(operands.toList()))
            .build()

        private fun not(operand: Operand): Operand = expression("not", operand)
    }
}
