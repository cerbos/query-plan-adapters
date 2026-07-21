package dev.cerbos.queryplan.springdata;

import java.util.Map;

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

    record Field(String jpaPath) implements AttributeMapping {}

    record Relation(String joinAttribute, String defaultMemberField, Map<String, AttributeMapping> fields)
            implements AttributeMapping {}
}
