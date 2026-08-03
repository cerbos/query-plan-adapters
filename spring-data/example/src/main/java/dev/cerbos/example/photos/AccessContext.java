package dev.cerbos.example.photos;

import dev.cerbos.sdk.builders.AttributeValue;
import dev.cerbos.sdk.builders.Principal;

import java.util.Set;

public record AccessContext(String userId, String role, String tenantId,
                            Set<String> groups, Set<String> interests) {

    public AccessContext {
        groups = Set.copyOf(groups);
        interests = Set.copyOf(interests);
    }

    Principal toPrincipal() {
        AttributeValue[] groupValues = groups.stream()
                .map(group -> tenantId + ":" + group)
                .sorted()
                .map(AttributeValue::stringValue)
                .toArray(AttributeValue[]::new);
        AttributeValue[] interestValues = interests.stream()
                .sorted()
                .map(AttributeValue::stringValue)
                .toArray(AttributeValue[]::new);
        return Principal.newInstance(userId)
                .withRoles(role)
                .withAttribute("tenantId", AttributeValue.stringValue(tenantId))
                .withAttribute("groups", AttributeValue.listValue(groupValues))
                .withAttribute("interests", AttributeValue.listValue(interestValues));
    }
}
