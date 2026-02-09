package dev.cerbos.queryplan.elasticsearch;

import java.util.Map;

@FunctionalInterface
public interface OperatorFunction {
    Map<String, Object> apply(String field, Object value);
}
