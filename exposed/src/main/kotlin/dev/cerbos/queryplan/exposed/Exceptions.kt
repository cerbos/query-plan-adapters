package dev.cerbos.queryplan.exposed

/**
 * The plan violates the planner's wire contract: wrong arity, a lambda whose second operand is not
 * a variable, a conditional plan with no condition, an unknown filter kind, a protobuf value with
 * no kind. No Cerbos planner output should produce one.
 */
public class MalformedPlanException internal constructor(message: String, cause: Throwable? = null) :
    IllegalArgumentException(message, cause) {
    private companion object {
        private const val serialVersionUID: Long = 1L
    }
}

/**
 * The caller's mapping came up short: a variable nothing maps, a relation where a scalar is needed,
 * a column where a collection operator needs a relation, a temporal column whose type does not pin
 * an absolute instant. The fix is a mapping change, not a policy rewrite, which is why this is a
 * type of its own.
 */
public class UnmappedAttributeException internal constructor(message: String) :
    IllegalArgumentException(message) {
    private companion object {
        private const val serialVersionUID: Long = 1L
    }
}

/**
 * A well-formed plan this adapter cannot express faithfully in SQL. It is raised instead of a
 * best-effort filter, because a wrong filter returns rows the PDP denies. Catch this type to route
 * an inexpressible shape elsewhere (per-row `check()`, another store) without matching on the
 * message.
 */
public class UnsupportedPlanShapeException internal constructor(message: String) :
    IllegalArgumentException(message) {
    private companion object {
        private const val serialVersionUID: Long = 1L
    }
}
