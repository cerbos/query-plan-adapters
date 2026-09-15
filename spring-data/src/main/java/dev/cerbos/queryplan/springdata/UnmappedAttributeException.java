package dev.cerbos.queryplan.springdata;

/**
 * The plan uses an attribute in a way the caller's {@link AttributeMapping mapping} does not
 * cover: a variable the mapping does not name, a {@link AttributeMapping.Relation} where the
 * operator needs a scalar column, a {@link AttributeMapping.Field} where a collection operator
 * needs a relation, a temporal column whose Java type does not pin the absolute instant it
 * stores, or two sides of one comparison declared under different NULL conventions.
 *
 * <p>The adapter is handed a plan and a mapping, never the entity model's meaning, so it cannot
 * guess a column and never uses a plan variable verbatim as a path. Every one of these is a gap
 * in what the caller declared rather than a limitation of the Criteria API, which is why it is
 * a type of its own: the fix is a mapping change, not a policy rewrite.
 */
public final class UnmappedAttributeException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    public UnmappedAttributeException(String message) {
        super(message);
    }
}
