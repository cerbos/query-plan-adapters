package dev.cerbos.queryplan.springdata;

import java.util.Map;
import java.util.Objects;

public sealed interface AttributeMapping permits AttributeMapping.Field, AttributeMapping.Relation {

    static Field field(String jpaPath) {
        return new Field(jpaPath);
    }

    static Relation relation(String joinAttribute) {
        return new Relation(joinAttribute, null, Map.of());
    }

    static Relation relation(String joinAttribute, String defaultMemberField) {
        return new Relation(joinAttribute, defaultMemberField, Map.of());
    }

    static Relation relation(String joinAttribute, Map<String, AttributeMapping> fields) {
        return new Relation(joinAttribute, null, fields);
    }

    static Relation relation(String joinAttribute, String defaultMemberField, Map<String, AttributeMapping> fields) {
        return new Relation(joinAttribute, defaultMemberField, fields);
    }

    record Field(String jpaPath) implements AttributeMapping {
        public Field {
            Objects.requireNonNull(jpaPath, "jpaPath");
        }
    }

    /**
     * A collection-valued mapping. {@code defaultMemberField} may be {@code null} (the joined
     * element itself is the member value); {@code joinAttribute} and {@code fields} must not be.
     * The {@code fields} map is defensively copied, so later mutation of the caller's map
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
