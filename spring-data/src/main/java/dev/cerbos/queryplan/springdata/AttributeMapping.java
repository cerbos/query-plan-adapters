/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import java.util.Map;
import java.util.Objects;

/**
 * Maps one plan variable (e.g. {@code request.resource.attr.ownerId} or
 * {@code request.resource.id}) to a scalar {@linkplain Field path} or a collection
 * {@linkplain Relation relation} on the JPA model. The map passed to
 * {@link SpringDataQueryPlanAdapter#toSpecification(dev.cerbos.sdk.PlanResourcesResult, Map)
 * toSpecification} is keyed by the full variable name:
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
 *   <tr><td>{@link Relation#withPositionField relation(...).withPositionField("position")}</td>
 *       <td>Any of the relations above, with the member field holding each element's list
 *           index, for positional reads such as {@code R.attr.tags[0]}</td></tr>
 * </table>
 *
 * <p>A variable that is missing from the map, or mapped in a way the operator cannot use,
 * throws {@link UnmappedAttributeException}.
 */
public sealed interface AttributeMapping permits AttributeMapping.Field, AttributeMapping.Relation {

    /**
     * Maps an attribute to a scalar JPA path on the entity.
     *
     * <p>A dotted path can go through {@code @Embedded} objects and to-one associations, e.g.
     * {@code field("details.pixelWidth")} or {@code field("owner.id")}. Associations are
     * LEFT-joined, so a missing one makes only its own comparison UNKNOWN.
     *
     * @param jpaPath entity property name, or a dot-separated path
     * @return the scalar mapping
     */
    static Field field(String jpaPath) {
        return new Field(jpaPath, null);
    }

    /**
     * Maps an attribute to a scalar JPA path whose column can be NULL, and declares how the
     * caller sends that NULL to {@code check()}. It overrides the call-level
     * {@link NullAttributeRepresentation}, so one call can mix both conventions.
     *
     * <p>Under {@link NullAttributeRepresentation#EXPLICIT}, {@code eq}, {@code ne} and
     * {@code in} are rendered so a NULL column gives a definite result, as CEL does, instead of
     * SQL UNKNOWN. Under {@link NullAttributeRepresentation#OMITTED}, {@code eq} and {@code ne}
     * against a bare null are UNKNOWN for a NULL column under both polarities, and other null
     * operands against this attribute are rejected.
     *
     * @param jpaPath entity property name, or a dot-separated path
     * @param nullAttributeRepresentation how a NULL in this column reaches {@code check()}
     * @return the scalar mapping
     */
    static Field field(String jpaPath, NullAttributeRepresentation nullAttributeRepresentation) {
        return new Field(jpaPath, Objects.requireNonNull(
                nullAttributeRepresentation, "nullAttributeRepresentation"));
    }

    /**
     * Maps an attribute to a collection whose elements are the compared values, typically an
     * {@code @ElementCollection} of strings.
     *
     * @param joinAttribute the entity's collection property name
     * @return the relation mapping
     */
    static Relation relation(String joinAttribute) {
        return new Relation(joinAttribute, null, Map.of());
    }

    /**
     * Maps an attribute to an entity collection whose {@code defaultMemberField} is used
     * wherever the policy treats elements as bare values. With {@code relation("tags", "name")},
     * {@code "urgent" in R.attr.tags} compares against {@code tag.name}.
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
     * Maps an attribute to an entity collection with mappings for the member fields that lambda
     * bodies reference. With {@code relation("tags", Map.of("name", field("name")))},
     * {@code t.name} in {@code R.attr.tags.exists(t, t.name == "x")} resolves on the member
     * entity. A nested entry may itself be a {@code relation(...)}, for multi-hop chains.
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
     * String) default member field} and {@linkplain #relation(String, Map) nested field
     * mappings}.
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
     * Scalar mapping. {@code nullAttributeRepresentation} is null when the mapping declares no
     * NULL convention. Create via {@link #field(String)} or
     * {@link #field(String, NullAttributeRepresentation)}.
     */
    record Field(String jpaPath, NullAttributeRepresentation nullAttributeRepresentation)
            implements AttributeMapping {
        public Field {
            Objects.requireNonNull(jpaPath, "jpaPath");
        }

        /** A mapping that declares no NULL convention; see {@link #field(String)}. */
        public Field(String jpaPath) {
            this(jpaPath, null);
        }
    }

    /**
     * Collection mapping. {@code defaultMemberField} and {@code positionField} may be null;
     * {@code fields} is copied. Create via the {@code relation(...)} factory methods, and
     * {@link #withPositionField} to declare the element order.
     */
    record Relation(String joinAttribute, String defaultMemberField,
                    Map<String, AttributeMapping> fields, String positionField)
            implements AttributeMapping {
        public Relation {
            Objects.requireNonNull(joinAttribute, "joinAttribute");
            Objects.requireNonNull(fields, "fields");
            fields = Map.copyOf(fields);
        }

        /** A relation with no declared element order. */
        public Relation(String joinAttribute, String defaultMemberField,
                        Map<String, AttributeMapping> fields) {
            this(joinAttribute, defaultMemberField, fields, null);
        }

        /**
         * Declares the member-entity field holding each element's zero-based position in the
         * list the application sends to {@code check()}, so a positional read such as
         * {@code R.attr.tags[0] == "x"} can be translated. Without it, a relation has no order a
         * plan can name and positional reads throw {@link UnsupportedPlanShapeException}.
         *
         * <p>The field must hold exactly the list index: {@code 0} for the first element, one
         * row per position, no gaps. A read past the last position is a CEL error, which
         * denies.
         *
         * @param positionField member-entity field holding the element's list index
         * @return the relation mapping with its order declared
         */
        public Relation withPositionField(String positionField) {
            return new Relation(joinAttribute, defaultMemberField, fields,
                    Objects.requireNonNull(positionField, "positionField"));
        }
    }
}
