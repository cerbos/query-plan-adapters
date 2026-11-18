package dev.cerbos.queryplan.jpa.domains;

import dev.cerbos.queryplan.jpa.AttributeMapping;
import dev.cerbos.queryplan.jpa.JpaQueryPlanAdapter.Options;
import dev.cerbos.queryplan.jpa.NullAttributeRepresentation;

import java.util.List;

interface DomainMapping {

    String domain();

    List<Class<?>> entities();

    Class<?> root();

    default String idAttribute() {
        return "id";
    }

    Options options();

    static AttributeMapping.Field explicit(String path) {
        return AttributeMapping.field(path, NullAttributeRepresentation.EXPLICIT);
    }

    static AttributeMapping.Field omitted(String path) {
        return AttributeMapping.field(path, NullAttributeRepresentation.OMITTED);
    }
}
