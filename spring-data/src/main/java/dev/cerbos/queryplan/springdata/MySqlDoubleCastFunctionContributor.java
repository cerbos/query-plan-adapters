package dev.cerbos.queryplan.springdata;

import org.hibernate.boot.model.FunctionContributions;
import org.hibernate.boot.model.FunctionContributor;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MariaDBDialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.type.StandardBasicTypes;

/**
 * Hibernate {@link FunctionContributor} (discovered via {@link java.util.ServiceLoader};
 * see {@code META-INF/services/org.hibernate.boot.model.FunctionContributor}) that keeps
 * the adapter's double-space arithmetic IEEE-faithful on MySQL.
 *
 * <p><strong>Why this exists.</strong> The adapter deliberately lowers CEL arithmetic
 * comparisons to SQL computed in IEEE double space, because that is exactly how the Cerbos
 * PDP evaluates them at check time (CEL attribute arithmetic is always double-typed). On
 * MySQL two defaults conspire to break that:
 *
 * <ul>
 *   <li>Hibernate's {@code MySQLDialect} renders {@code cast(x as Double)} as
 *       {@code cast(x as decimal(53,20))} — its {@code castType} still assumes MySQL cannot
 *       cast to DOUBLE, which has been supported since MySQL 8.0.17.</li>
 *   <li>MySQL Connector/J's DEFAULT client-side prepared statements interpolate double bind
 *       parameters into the statement text, where MySQL parses them as exact DECIMAL
 *       literals.</li>
 * </ul>
 *
 * <p>Together the adapter's "double" arithmetic becomes all-DECIMAL and evaluates exactly:
 * {@code 3 * 0.1 == 0.3} is TRUE in decimal but FALSE in CEL/IEEE double
 * ({@code 0.30000000000000004}), so the SQL filter returns rows the PDP's {@code check()}
 * API denies. Verified on MySQL 8.4: {@code CAST(3 AS DECIMAL(53,20)) * 0.1 = 0.3} yields
 * 1, {@code CAST(3 AS DOUBLE) * 0.1 = 0.3} yields 0.
 *
 * <p><strong>What it does.</strong> On MySQL 8.0.17+ this contributor registers the
 * {@value #FUNCTION_NAME} function rendering {@code cast(?1 as double)}. The adapter routes
 * every column entering arithmetic through it (instead of {@code cb.toDouble}), which makes
 * every arithmetic node double-typed: MySQL promotes any expression with an approximate
 * operand to double, so the interpolated DECIMAL literals no longer drag the evaluation
 * into exact decimal — client- and server-side prepared statements then agree with CEL.
 *
 * <p><strong>Where it does nothing</strong> (the adapter falls back to {@code cb.toDouble},
 * i.e. exactly the pre-existing behavior):
 *
 * <ul>
 *   <li>H2 and PostgreSQL: their dialects already render IEEE-correct casts
 *       ({@code cast(x as float(53))}); registering nothing keeps their SQL byte-identical.</li>
 *   <li>MySQL older than 8.0.17: {@code CAST(... AS DOUBLE)} is unsupported there — such
 *       deployments must set {@code useServerPrepStmts=true} instead (README, "MySQL:
 *       keeping arithmetic IEEE-faithful").</li>
 *   <li>MariaDB: ships its own dialect lineage and JDBC driver whose literal behavior this
 *       adapter has not verified; the README's server-side-prepared-statements guidance
 *       applies.</li>
 *   <li>Non-Hibernate JPA providers: this class never loads (it is only reachable via
 *       Hibernate's ServiceLoader discovery and the adapter's classpath-guarded probe).</li>
 * </ul>
 */
public final class MySqlDoubleCastFunctionContributor implements FunctionContributor {

    /**
     * Name of the registered cast function. Referenced from
     * {@link SpringDataQueryPlanAdapter} only as a compile-time String constant (inlined by
     * javac) or from Hibernate-guarded code, so the adapter never triggers loading this
     * Hibernate-dependent class on non-Hibernate runtimes.
     */
    static final String FUNCTION_NAME = "cerbos_ieee_double";

    @Override
    public void contributeFunctions(FunctionContributions functionContributions) {
        Dialect dialect = functionContributions.getDialect();
        if (!(dialect instanceof MySQLDialect) || dialect instanceof MariaDBDialect) {
            return;
        }
        if (!dialect.getVersion().isSameOrAfter(8, 0, 17)) {
            // CAST(... AS DOUBLE) arrived in MySQL 8.0.17. NOTE: when Hibernate cannot read
            // JDBC metadata (hibernate.boot.allow_jdbc_metadata_access=false with only
            // hibernate.dialect set), the dialect reports its minimum supported version and
            // the registration is skipped — fail-safe to the documented
            // useServerPrepStmts=true guidance rather than emitting SQL an older server
            // rejects.
            return;
        }
        functionContributions.getFunctionRegistry()
                .patternDescriptorBuilder(FUNCTION_NAME, "cast(?1 as double)")
                .setExactArgumentCount(1)
                .setInvariantType(functionContributions.getTypeConfiguration()
                        .getBasicTypeRegistry().resolve(StandardBasicTypes.DOUBLE))
                .register();
    }

    @Override
    public int ordinal() {
        return 600; // library range (500–1000); the name is namespaced, collisions are not expected
    }
}
