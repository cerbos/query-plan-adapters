package dev.cerbos.queryplan.elasticsearch.domains;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.ScalarType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

interface EsDomain {

    String name();

    /** The index mapping's {@code properties}, as JSON. */
    String properties();

    Options options();

    /**
     * Options derived from the index mapping: every leaf mapped under its own attribute name, its
     * CEL type read from the field type, and every {@code nested} field declared as a nested path.
     */
    static Options fromMapping(String properties) {
        JsonNode props;
        try {
            props = new ObjectMapper().readTree(properties);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        Map<String, String> fields = new HashMap<>();
        Map<String, ScalarType> types = new HashMap<>();
        Set<String> nested = new HashSet<>();
        walk(props, "", fields, types, nested);
        fields.put("request.resource.id", "id");
        types.put("id", ScalarType.STRING);
        return Options.of(fields).withScalarTypes(types).withNestedPaths(nested);
    }

    private static void walk(JsonNode props, String prefix, Map<String, String> fields,
                             Map<String, ScalarType> types, Set<String> nested) {
        for (Iterator<Map.Entry<String, JsonNode>> it = props.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            String path = prefix + e.getKey();
            JsonNode def = e.getValue();
            String type = def.path("type").asText("object");
            if (def.has("properties")) {
                if (type.equals("nested")) {
                    nested.add(path);
                    fields.put("request.resource.attr." + path, path);
                }
                walk(def.get("properties"), path + ".", fields, types, nested);
                continue;
            }
            fields.put("request.resource.attr." + path, path);
            types.put(path, switch (type) {
                case "keyword" -> ScalarType.STRING;
                case "boolean" -> ScalarType.BOOLEAN;
                case "date" -> ScalarType.TIMESTAMP;
                default -> ScalarType.NUMBER;
            });
        }
    }
}
