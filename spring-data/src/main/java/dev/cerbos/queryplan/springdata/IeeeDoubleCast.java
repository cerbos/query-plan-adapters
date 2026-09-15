package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.CriteriaBuilder;

/**
 * Classpath-guarded probe for the {@code cerbos_ieee_double} MySQL cast function
 * registered by {@link MySqlDoubleCastFunctionContributor}. The adapter itself depends
 * only on Jakarta Persistence; everything that touches Hibernate types lives in the
 * nested {@link Probe} class, which is only loaded after {@code hibernate-core} has been
 * confirmed present — on non-Hibernate providers {@link #isRegistered} is a constant
 * {@code false} and the caller keeps the portable {@code cb.toDouble} path.
 *
 * <p>Its one caller is {@link ArithmeticTranslator#toIeeeDouble}; it is a separate type so
 * that the Hibernate class reference stays out of every collaborator's constant pool.
 */
final class IeeeDoubleCast {

    private IeeeDoubleCast() {}

    private static final boolean HIBERNATE_PRESENT = detectHibernate();

    private static boolean detectHibernate() {
        try {
            Class.forName("org.hibernate.query.sqm.NodeBuilder", false,
                    IeeeDoubleCast.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    /** True when the current CriteriaBuilder's session factory has the function registered. */
    static boolean isRegistered(CriteriaBuilder cb) {
        return HIBERNATE_PRESENT && Probe.isRegistered(cb);
    }

    /** The only code that references Hibernate types; never loaded without hibernate-core. */
    private static final class Probe {
        static boolean isRegistered(CriteriaBuilder cb) {
            return cb instanceof org.hibernate.query.sqm.NodeBuilder nb
                    && nb.getQueryEngine().getSqmFunctionRegistry().findFunctionDescriptor(
                            MySqlDoubleCastFunctionContributor.FUNCTION_NAME) != null;
        }
    }
}
