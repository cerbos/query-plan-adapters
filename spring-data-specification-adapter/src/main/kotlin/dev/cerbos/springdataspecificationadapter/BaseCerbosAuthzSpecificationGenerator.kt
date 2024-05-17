package dev.cerbos.springdataspecificationadapter

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.sdk.CerbosBlockingClient
import dev.cerbos.sdk.PlanResourcesResult
import dev.cerbos.sdk.builders.Resource

import jakarta.persistence.criteria.CriteriaBuilder
import jakarta.persistence.criteria.CriteriaQuery
import jakarta.persistence.criteria.Expression
import jakarta.persistence.criteria.JoinType
import jakarta.persistence.criteria.Predicate
import jakarta.persistence.criteria.Root
import jakarta.persistence.criteria.Subquery
import org.springframework.data.jpa.domain.Specification

import java.util.UUID


open class BaseCerbosAuthzSpecificationGenerator<T : Any>(
    private val cerbos: CerbosBlockingClient,
    private val principalRepository: PrincipalRepository,
    private val policyPathToType: Map<String, Class<*>>
) {

    fun specificationFor(id: String, resource: String, action: String): Specification<T> {

        val result: PlanResourcesResult = cerbos.plan(
            principalRepository.retrievePrincipalById(id)!!,
            Resource.newInstance(resource),
            action
        )
        return when {
            // no additional filtering needed
            result.isAlwaysAllowed -> return allowed()
            // don't generate a specification
            result.isAlwaysDenied -> throw RuntimeException()
            // generate a specification
            result.isConditional ->
                operandToSpecification(result.condition.get())

            else -> throw UnsupportedOperationException("Unsupported result $result")
        }
    }


    private fun operandToSpecification(
        op: Operand
    ): Specification<T> {
        return when (op.expression.operator) {

            "and" -> Specification.allOf(op.expression.operandsList.map { operandToSpecification(it) })
            "or" -> Specification { r: Root<T>, cq: CriteriaQuery<*>, cb: CriteriaBuilder ->
                val predicates = op.expression.operandsList.map { operand ->
                    val subquery = cq.subquery(Long::class.java)
                    subquery.from(r.javaType)
                    subquery
                        .select(cb.literal(1))
                        .where(operandToSpecification(operand).toPredicate(r, cq, cb))
                    cb.exists(subquery)
                }.toTypedArray()

                cb.or(*predicates)
            }
            "not" -> Specification.not(Specification.allOf(
                op.expression.operandsList.map { operandToSpecification(it) }))
            "ne" -> Specification { r: Root<T>, cq: CriteriaQuery<*>, cb: CriteriaBuilder ->
                cb.not(equalityPredicate(op).toPredicate(r, cq, cb))
            }
            "eq" -> equalityPredicate(op)
            else -> throw UnsupportedOperationException("Unexpected operand $op")
        }
    }


    private fun equalityPredicate(
        op: Operand
    ): Specification<T> =
        Specification { r: Root<T>, _, cb ->
            parseEqualityOperands(r, cb, op.expression.operandsList[0], op.expression.operandsList[1])
        }




    private fun parseEqualityOperands(r: Root<T>, cb: CriteriaBuilder, left: Operand, right: Operand): Predicate? {
        return when {
            left.hasVariable() && right.hasValue() -> {
                val rightValue = policyPathToType[left.variable]?.let { valueToType(it, right.value) }
                cb.equal(walkPath(r, cb, left), cb.literal(rightValue))
            }
            left.hasVariable() && right.hasVariable() ->
                cb.equal(walkPath(r, cb, left), walkPath(r, cb, right))

            else -> TODO()
        }
    }

    private fun <T> walkPath(r: Root<T>, cb: CriteriaBuilder, op: Operand): Expression<Any> {
        return when {
            op.hasVariable() -> {

                val removePrefix = op.variable.removePrefix("request.resource.attr.")

                // Warning implicitly any depth > 1 to a nesting problem will be solved by a join
                // TODO explicitly map join and attribute comparison
                if (removePrefix.contains(".")) {
                    val split = removePrefix.split(".")
                    // TODO forced to used a left join due to the way I've handled OR - this needs work but it's black-box ok
                    return r.join<Any, Any>(split[0], JoinType.INNER).get(split[1])

                }
                return r.get(removePrefix)

            }

            else -> TODO()
        }

    }


    private fun valueToType(clazz: Class<*>, value: Value): Any {
        return when (clazz) {
            Boolean::class.java -> value.boolValue
            UUID::class.java -> UUID.fromString(value.stringValue)
            String::class.java -> value.stringValue
            else -> TODO()
        }
    }

    private fun <T> allowed(): Specification<T> = Specification { _: Root<T>, _, cb ->
        cb.equal(cb.literal("1"), "1")

    }

}