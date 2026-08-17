package dev.cerbos.example.demo;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

final class DemoCases {
    record Pagination(int pageSize, List<Integer> pageSizes) {}
    record Expected(String kind, List<String> ids) {}
    record DemoCase(String id, String operation, String principal, String action,
                    Pagination pagination, Expected expected) {}
    private record File(int schemaVersion, List<DemoCase> cases) {}

    private DemoCases() {}

    static List<DemoCase> read(Path path) {
        try {
            File file = new ObjectMapper().readValue(path.toFile(), File.class);
            if (file.schemaVersion() != 1) {
                throw new IllegalStateException("demo cases must use schemaVersion 1");
            }
            Set<String> ids = file.cases().stream()
                    .map(DemoCase::id).collect(Collectors.toSet());
            if (ids.size() != file.cases().size()) {
                throw new IllegalStateException("demo case ids must be unique");
            }
            for (DemoCase demoCase : file.cases()) {
                String expectedId = demoCase.operation() + "/" + demoCase.principal()
                        + "/" + demoCase.action();
                if (!expectedId.equals(demoCase.id())) {
                    throw new IllegalStateException("invalid demo case id " + demoCase.id());
                }
            }
            return List.copyOf(file.cases());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
