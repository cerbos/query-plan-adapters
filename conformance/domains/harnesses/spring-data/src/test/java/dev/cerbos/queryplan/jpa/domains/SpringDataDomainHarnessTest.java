package dev.cerbos.queryplan.jpa.domains;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.jpa.MalformedPlanException;
import dev.cerbos.queryplan.jpa.UnmappedAttributeException;
import dev.cerbos.queryplan.jpa.UnsupportedPlanShapeException;
import dev.cerbos.queryplan.springdata.SpringDataQueryPlanAdapter;

import jakarta.persistence.EntityManager;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.Test;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.support.SimpleJpaRepository;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class SpringDataDomainHarnessTest {

    static final List<DomainMapping> DOMAINS = List.of(
            new Healthcare(), new Fintech(), new Logistics(), new Education(), new Legal(),
            new Retail(), new Hr(), new Media(), new Iot(), new Government());

    static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    final Path domainsDir = Path.of(System.getProperty("user.dir"), "..", "..").normalize();

    @Test
    void replayDomains() throws Exception {
        List<ObjectNode> results = new ArrayList<>();
        for (DomainMapping d : DOMAINS) {
            String url = "jdbc:h2:mem:" + d.domain() + ";DB_CLOSE_DELAY=-1";
            try (Connection c = DriverManager.getConnection(url, "sa", ""); Statement s = c.createStatement()) {
                for (String stmt : Files.readString(domainsDir.resolve(d.domain()).resolve("generated/sql/h2.sql")).split(";\\s*\\n")) {
                    String body = stmt.lines().filter(l -> !l.startsWith("--")).collect(Collectors.joining("\n")).strip();
                    if (!body.isEmpty()) {
                        s.execute(body);
                    }
                }
            }
            Configuration cfg = new Configuration();
            d.entities().forEach(cfg::addAnnotatedClass);
            cfg.setProperty("hibernate.connection.url", url);
            cfg.setProperty("hibernate.connection.username", "sa");
            cfg.setProperty("hibernate.dialect", "org.hibernate.dialect.H2Dialect");
            cfg.setProperty("hibernate.globally_quoted_identifiers", "true");
            cfg.setProperty("hibernate.physical_naming_strategy",
                    "org.hibernate.boot.model.naming.CamelCaseToUnderscoresNamingStrategy");
            cfg.setProperty("hibernate.jdbc.time_zone", "UTC");
            try (SessionFactory sf = cfg.buildSessionFactory();
                 Stream<Path> files = Files.walk(domainsDir.resolve(d.domain()).resolve("golden"))) {
                for (Path p : files.filter(f -> f.toString().endsWith(".json")).sorted().toList()) {
                    results.add(replay(sf, d, JSON.readTree(p.toFile())));
                }
            }
        }
        results.sort(Comparator.comparing((ObjectNode n) -> n.get("domain").asText())
                .thenComparing(n -> n.get("pdp").asText())
                .thenComparing(n -> n.get("principal").asText())
                .thenComparing(n -> n.get("action").asText()));
        ObjectNode out = JSON.createObjectNode();
        out.put("adapter", "spring-data");
        out.put("store", "h2");
        ArrayNode arr = out.putArray("results");
        results.forEach(arr::add);
        Path dir = domainsDir.resolve("results/spring-data");
        Files.createDirectories(dir);
        JSON.writeValue(dir.resolve("h2.json").toFile(), out);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    ObjectNode replay(SessionFactory sf, DomainMapping d, JsonNode golden) {
        ObjectNode r = JSON.createObjectNode();
        r.put("domain", d.domain());
        r.put("pdp", golden.get("pdp").asText());
        r.put("principal", golden.get("principalName").asText());
        r.put("action", golden.get("action").asText());
        Set<String> allowed = new TreeSet<>();
        golden.get("allowed").forEach(n -> allowed.add(n.asText()));
        Set<String> returned = new TreeSet<>();
        String verdict;
        String detail = "";
        try (EntityManager em = sf.createEntityManager()) {
            PlanResourcesFilter.Builder filter = PlanResourcesFilter.newBuilder();
            JsonFormat.parser().merge(golden.get("plan").toString(), filter);
            Specification spec = SpringDataQueryPlanAdapter.toSpecification(
                    PlanResourcesResponse.newBuilder().setFilter(filter).build(), d.options());
            SimpleJpaRepository repo = new SimpleJpaRepository(d.root(), em);
            for (Object entity : repo.findAll(spec)) {
                returned.add(String.valueOf(sf.getPersistenceUnitUtil().getIdentifier(entity)));
            }
            if (!allowed.containsAll(returned)) {
                verdict = "over-grant";
            } else if (returned.equals(allowed)) {
                verdict = "pass";
            } else {
                verdict = "under-grant";
            }
        } catch (UnsupportedPlanShapeException | UnmappedAttributeException | MalformedPlanException e) {
            verdict = "refused";
            detail = e.getClass().getSimpleName() + ": " + e.getMessage();
        } catch (Exception e) {
            verdict = "error";
            detail = e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        r.put("verdict", verdict);
        ArrayNode ret = r.putArray("returned");
        returned.forEach(ret::add);
        ArrayNode all = r.putArray("allowed");
        allowed.forEach(all::add);
        r.put("detail", detail);
        return r;
    }
}
