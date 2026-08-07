package dev.cerbos.queryplan.springdata;

import java.util.Map;
import java.util.Objects;

/**
 * Maps one Cerbos attribute reference (the {@code variable} name in a query plan, e.g.
 * {@code request.resource.attr.ownerId} or {@code request.resource.id}) onto the JPA model,
 * either as a scalar {@linkplain Field path} or as a collection {@linkplain Relation
 * relation}. The mapper passed to
 * {@link SpringDataQueryPlanAdapter#toSpecification(dev.cerbos.sdk.PlanResourcesResult, Map)
 * toSpecification} is keyed by the full attribute reference:
 *
 * <pre>{@code
 * Map<String, AttributeMapping> MAPPING = Map.of(
 *     "request.resource.attr.ownerId",    AttributeMapping.field("owner.id"),
 *     "request.resource.attr.department", AttributeMapping.field("department"),
 *     "request.resource.attr.tags",       AttributeMapping.relation("tags", Map.of(
 *         "name", AttributeMapping.field("name"))));
 * }</pre>
 *
 * <p>Choosing a helper:
 *
 * <table border="1">
 *   <caption>Mapping helpers</caption>
 *   <tr><th>Helper</th><th>Use for</th></tr>
 *   <tr><td>{@link #field(String) field("aPath")}</td>
 *       <td>Simple column or {@code @Embedded} dotted path</td></tr>
 *   <tr><td>{@link #relation(String) relation("tags")}</td>
 *       <td>{@code @ElementCollection<String>} (bare values)</td></tr>
 *   <tr><td>{@link #relation(String, String) relation("tags", "name")}</td>
 *       <td>{@code @OneToMany} collection whose default member field is {@code name}</td></tr>
 *   <tr><td>{@link #relation(String, Map) relation("tags", Map.of("name", field("name")))}</td>
 *       <td>{@code @OneToMany<Tag>} with explicit nested field mapping</td></tr>
 *   <tr><td>{@link #relation(String, String, Map) relation("tags", "name", Map.of(...))}</td>
 *       <td>Both: a default member field for bare-value operators plus nested mappings for
 *           lambda bodies</td></tr>
 * </table>
 *
 * <p>Plan variables that are missing from the mapper, or whose mapping cannot be resolved
 * against the entity model (e.g. a {@code Relation} used where a scalar path is required),
 * cause translation to throw {@link IllegalArgumentException} — the adapter fails closed
 * rather than guessing a column.
 */
public sealed interface AttributeMapping permits AttributeMapping.Field, AttributeMapping.Relation {

    /**
     * Maps an attribute to a scalar JPA path on the entity.
     *
     * <p>{@code jpaPath} is resolved segment-by-segment via {@code Path.get(...)}, so dotted
     * paths traverse {@code @Embedded} objects (and to-one associations):
     * {@code field("details.pixelWidth")}, {@code field("owner.id")}. Use this for any
     * attribute compared as a single value ({@code eq}/{@code ne}/ordering/LIKE/temporal
     * comparisons, scalar {@code in}, ...).
     *
     * @param jpaPath entity property name, or a dot-separated path through embeddables
     * @return the scalar mapping
     */
    static Field field(String jpaPath) {
        return new Field(jpaPath, null);
    }

    /**
     * Maps an attribute to a scalar JPA path, declaring that its column can be SQL NULL and
     * how the caller represents that NULL in the attributes it sends to {@code check()}.
     *
     * <p>Declaring it asserts both facts; the {@link #field(String) one-argument form} means
     * "treat this column as NOT NULL", which is the historical rendering.
     *
     * <p>This is per attribute rather than per call because one policy suite can legitimately
     * mix the two conventions — the same column can be mapped twice, sent as an explicit null
     * under one attribute name and omitted under another. The call-level
     * {@link NullAttributeRepresentation} cannot express that, which is what made
     * <a href="https://github.com/cerbos/query-plan-adapters/issues/308">#308</a> unfixable
     * with <a href="https://github.com/cerbos/query-plan-adapters/issues/302">#302</a>'s
     * option alone.
     *
     * <p>Under {@link NullAttributeRepresentation#EXPLICIT} CEL holds a null VALUE, so
     * {@code null != "x"} is TRUE and {@code null == "x"} is FALSE, both definite, while SQL
     * answers UNKNOWN and excludes the row under BOTH polarities. The equality family
     * ({@code eq}, {@code ne}, {@code in}) is therefore rendered so it can never be UNKNOWN.
     * Ordering and string operators are left alone: a null receiver raises a no-overload error
     * in CEL, which denies exactly as UNKNOWN does.
     *
     * <p>Under {@link NullAttributeRepresentation#OMITTED} the rendering is unchanged; what
     * the declaration adds is the same null-operand rejection the call-level option performs,
     * scoped to this attribute.
     *
     * @param jpaPath entity property name, or a dot-separated path through embeddables
     * @param nullAttributeRepresentation how a NULL in this column reaches {@code check()}
     * @return the scalar mapping
     */
    static Field field(String jpaPath, NullAttributeRepresentation nullAttributeRepresentation) {
        return new Field(jpaPath, Objects.requireNonNull(
                nullAttributeRepresentation, "nullAttributeRepresentation"));
    }

    /**
     * Maps an attribute to a bare-value collection — typically an
     * {@code @ElementCollection<String>} — whose elements ARE the compared values.
     *
     * <p>Collection operators ({@code in}, {@code hasIntersection}, {@code exists}-family
     * lambdas over the bare element, {@code size(...)}) translate to correlated subqueries
     * against the collection table, comparing the element itself.
     *
     * @param joinAttribute the entity's collection property name
     * @return the relation mapping
     */
    static Relation relation(String joinAttribute) {
        return new Relation(joinAttribute, null, Map.of());
    }

    /**
     * Maps an attribute to an entity collection ({@code @OneToMany}) whose
     * {@code defaultMemberField} stands in for the member element wherever the policy treats
     * the collection as a list of bare values.
     *
     * <p>Example: with {@code relation("tags", "name")}, the policy expression
     * {@code "urgent" in R.attr.tags} compares against {@code tag.name} rather than the
     * {@code Tag} entity itself.
     *
     * @param joinAttribute the entity's collection property name
     * @param defaultMemberField member-entity field used when the policy addresses the
     *        element as a bare value
     * @return the relation mapping
     */
    static Relation relation(String joinAttribute, String defaultMemberField) {
        return new Relation(joinAttribute, defaultMemberField, Map.of());
    }

    /**
     * Maps an attribute to an entity collection ({@code @OneToMany}) with explicit mappings
     * for the member fields referenced inside lambda bodies.
     *
     * <p>Example: {@code relation("tags", Map.of("name", field("name")))} lets
     * {@code R.attr.tags.exists(t, t.name == "x")} resolve {@code t.name} on the member
     * entity. Nested {@code fields} entries may themselves be {@code relation(...)} mappings,
     * supporting multi-hop chains ({@code R.attr.categories.subCategories...}).
     *
     * @param joinAttribute the entity's collection property name
     * @param fields policy-facing member field name → mapping on the member entity
     * @return the relation mapping
     */
    static Relation relation(String joinAttribute, Map<String, AttributeMapping> fields) {
        return new Relation(joinAttribute, null, fields);
    }

    /**
     * Maps an attribute to an entity collection with both a {@linkplain #relation(String,
     * String) default member field} (for bare-value operators such as {@code in} /
     * {@code hasIntersection}) and {@linkplain #relation(String, Map) explicit nested field
     * mappings} (for lambda bodies).
     *
     * @param joinAttribute the entity's collection property name
     * @param defaultMemberField member-entity field used when the policy addresses the
     *        element as a bare value
     * @param fields policy-facing member field name → mapping on the member entity
     * @return the relation mapping
     */
    static Relation relation(String joinAttribute, String defaultMemberField, Map<String, AttributeMapping> fields) {
        return new Relation(joinAttribute, defaultMemberField, fields);
    }

    /**
     * Scalar mapping: {@code jpaPath} is an entity property name or a dot-separated
     * {@code @Embedded}/to-one path, resolved via {@code Path.get(...)} at translation time.
     * {@code nullAttributeRepresentation} is nullable and declares this column's NULL
     * convention when set. Create via {@link #field(String)} or
     * {@link #field(String, NullAttributeRepresentation)}.
     */
    record Field(String jpaPath, NullAttributeRepresentation nullAttributeRepresentation)
            implements AttributeMapping {
        public Field {
            Objects.requireNonNull(jpaPath, "jpaPath");
        }

        /** A mapping that declares no NULL convention — see {@link #field(String)}. */
        public Field(String jpaPath) {
            this(jpaPath, null);
        }
    }

    /**
     * Collection mapping: {@code joinAttribute} names the entity's collection property;
     * {@code defaultMemberField} (nullable) stands in for the element when the policy treats
     * the collection as bare values; {@code fields} maps policy-facing member field names
     * used inside lambda bodies. Create via the {@code relation(...)} factory methods, whose
     * Javadoc describes when each combination applies.
     *
     * <p>The {@code fields} map is defensively copied, so later mutation of the caller's map
     * cannot silently change which columns the authorization filter resolves.
     */
    record Relation(String joinAttribute, String defaultMemberField, Map<String, AttributeMapping> fields)
            implements AttributeMapping {
        public Relation {
            Objects.requireNonNull(joinAttribute, "joinAttribute");
            Objects.requireNonNull(fields, "fields");
            fields = Map.copyOf(fields);
        }
    }
}
