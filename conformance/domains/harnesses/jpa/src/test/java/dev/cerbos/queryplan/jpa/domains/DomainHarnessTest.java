package dev.cerbos.queryplan.jpa.domains;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.jpa.JpaQueryPlanAdapter;
import dev.cerbos.queryplan.jpa.MalformedPlanException;
import dev.cerbos.queryplan.jpa.QueryPlanFilter;
import dev.cerbos.queryplan.jpa.UnmappedAttributeException;
import dev.cerbos.queryplan.jpa.UnsupportedPlanShapeException;

import jakarta.persistence.EntityManager;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Root;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.Test;

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
import java.util.stream.Stream;

class DomainHarnessTest {

    static final List<DomainMapping> DOMAINS = List.of(
            new Healthcare(), new Fintech(), new Logistics(), new Education(), new Legal(),
            new Retail(), new Hr(), new Media(), new Iot(), new Government());

    static final ThreadLocal<String> LAST_SQL = new ThreadLocal<>();

    static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    final String store = env("ADAPTER_DOMAIN_STORE", "h2");
    final Path domainsDir = Path.of(System.getProperty("user.dir"), "..", "..").normalize();

    static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : v;
    }

    @Test
    void replayDomains() throws Exception {
        Set<String> only = new TreeSet<>(List.of(env("ADAPTER_DOMAIN_ONLY", "").split(",")));
        only.remove("");
        List<ObjectNode> results = new ArrayList<>();
        for (DomainMapping d : DOMAINS) {
            if (!only.isEmpty() && !only.contains(d.domain())) {
                continue;
            }
            Path sql = domainsDir.resolve(d.domain()).resolve("generated/sql/" + dialectFile());
            String url = prepareDatabase(d.domain(), null);
            try {
                loadSql(url, sql);
            } catch (java.sql.SQLIntegrityConstraintViolationException e) {
                // Keys differing only by case need a binary collation on MySQL (government's DSS/dss).
                System.out.println("==> " + d.domain() + ": " + e.getMessage() + "; reloading under utf8mb4_0900_bin");
                url = prepareDatabase(d.domain(), "utf8mb4_0900_bin");
                loadSql(url, sql);
            }
            try (SessionFactory sf = sessionFactory(d, url)) {
                for (JsonNode golden : goldens(d.domain())) {
                    results.add(replay(sf, d, golden));
                }
            }
        }
        results.sort(Comparator.comparing((ObjectNode n) -> n.get("domain").asText())
                .thenComparing(n -> n.get("pdp").asText())
                .thenComparing(n -> n.get("principal").asText())
                .thenComparing(n -> n.get("action").asText()));
        ObjectNode out = JSON.createObjectNode();
        out.put("adapter", "jpa");
        out.put("store", store);
        ArrayNode arr = out.putArray("results");
        results.forEach(arr::add);
        Path dir = domainsDir.resolve("results/jpa");
        Files.createDirectories(dir);
        String suffix = only.isEmpty() ? "" : ".partial";
        JSON.writeValue(dir.resolve(store + suffix + ".json").toFile(), out);
        results.stream().collect(java.util.stream.Collectors.groupingBy(
                n -> n.get("verdict").asText(), java.util.stream.Collectors.counting()))
                .forEach((k, v) -> System.out.println("==> " + store + " " + k + ": " + v));
    }

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
        LAST_SQL.remove();
        try (EntityManager em = sf.createEntityManager()) {
            QueryPlanFilter filter = JpaQueryPlanAdapter.toFilter(plan(golden), d.options());
            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaQuery<Object> q = cb.createQuery(Object.class);
            Root<?> root = q.from(d.root());
            q.select(root.get(d.idAttribute())).where(filter.toPredicate(root, q, cb));
            for (Object id : em.createQuery(q).getResultList()) {
                returned.add(String.valueOf(id));
            }
            if (!allowed.containsAll(returned)) {
                verdict = "over-grant";
                Set<String> extra = new TreeSet<>(returned);
                extra.removeAll(allowed);
                detail = "extra " + extra + "; sql: " + LAST_SQL.get();
            } else if (returned.equals(allowed)) {
                verdict = "pass";
            } else {
                verdict = "under-grant";
                Set<String> missing = new TreeSet<>(allowed);
                missing.removeAll(returned);
                detail = "missing " + missing + "; sql: " + LAST_SQL.get();
            }
        } catch (UnsupportedPlanShapeException | UnmappedAttributeException | MalformedPlanException e) {
            verdict = "refused";
            detail = e.getClass().getSimpleName() + ": " + e.getMessage();
        } catch (RuntimeException e) {
            verdict = "error";
            Throwable root = e;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            detail = e.getClass().getSimpleName() + ": " + e.getMessage()
                    + (root != e ? " | cause " + root.getClass().getSimpleName() + ": " + root.getMessage() : "")
                    + (LAST_SQL.get() != null ? "; sql: " + LAST_SQL.get() : "");
        }
        r.put("verdict", verdict);
        ArrayNode ret = r.putArray("returned");
        returned.forEach(ret::add);
        ArrayNode all = r.putArray("allowed");
        allowed.forEach(all::add);
        r.put("detail", detail);
        return r;
    }

    static PlanResourcesResponse plan(JsonNode golden) throws RuntimeException {
        PlanResourcesFilter.Builder filter = PlanResourcesFilter.newBuilder();
        try {
            JsonFormat.parser().merge(golden.get("plan").toString(), filter);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return PlanResourcesResponse.newBuilder().setFilter(filter).build();
    }

    List<JsonNode> goldens(String domain) throws Exception {
        try (Stream<Path> files = Files.walk(domainsDir.resolve(domain).resolve("golden"))) {
            List<JsonNode> out = new ArrayList<>();
            for (Path p : files.filter(f -> f.toString().endsWith(".json")).sorted().toList()) {
                out.add(JSON.readTree(p.toFile()));
            }
            return out;
        }
    }

    String dialectFile() {
        return store.equals("h2") ? "h2.sql" : store + ".sql";
    }

    String prepareDatabase(String domain, String collation) throws Exception {
        switch (store) {
            case "h2":
                return "jdbc:h2:mem:" + domain + ";DB_CLOSE_DELAY=-1";
            case "postgres": {
                String base = "jdbc:postgresql://localhost:" + env("ADAPTER_DOMAIN_PG_PORT", "45432") + "/postgres";
                try (Connection c = DriverManager.getConnection(base, "postgres", "postgres");
                     Statement s = c.createStatement()) {
                    s.execute("DROP SCHEMA IF EXISTS " + domain + " CASCADE");
                    s.execute("CREATE SCHEMA " + domain);
                }
                return base + "?currentSchema=" + domain;
            }
            case "mysql": {
                String base = "jdbc:mysql://localhost:" + env("ADAPTER_DOMAIN_MYSQL_PORT", "43306") + "/";
                try (Connection c = DriverManager.getConnection(base, "root", "root");
                     Statement s = c.createStatement()) {
                    s.execute("DROP DATABASE IF EXISTS " + domain);
                    s.execute("CREATE DATABASE " + domain + (collation == null ? "" : " COLLATE " + collation));
                }
                return base + domain + "?connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true";
            }
            default:
                throw new IllegalArgumentException("unknown store " + store);
        }
    }

    String user() {
        return switch (store) {
            case "postgres" -> "postgres";
            case "mysql" -> "root";
            default -> "sa";
        };
    }

    String password() {
        return switch (store) {
            case "postgres" -> "postgres";
            case "mysql" -> "root";
            default -> "";
        };
    }

    void loadSql(String url, Path file) throws Exception {
        String sql = Files.readString(file);
        try (Connection c = DriverManager.getConnection(url, user(), password());
             Statement s = c.createStatement()) {
            for (String stmt : sql.split(";\\s*\\n")) {
                String body = stmt.lines().filter(l -> !l.startsWith("--")).reduce("", (a, b) -> a + "\n" + b).strip();
                if (!body.isEmpty()) {
                    s.execute(body);
                }
            }
        }
    }

    SessionFactory sessionFactory(DomainMapping d, String url) {
        Configuration cfg = new Configuration();
        d.entities().forEach(cfg::addAnnotatedClass);
        cfg.setProperty("hibernate.connection.url", url);
        cfg.setProperty("hibernate.connection.username", user());
        cfg.setProperty("hibernate.connection.password", password());
        cfg.setProperty("hibernate.dialect", switch (store) {
            case "postgres" -> "org.hibernate.dialect.PostgreSQLDialect";
            case "mysql" -> "org.hibernate.dialect.MySQLDialect";
            default -> "org.hibernate.dialect.H2Dialect";
        });
        cfg.setProperty("hibernate.hbm2ddl.auto", "none");
        // H2 loads the shared SQL's quoted lowercase names; elsewhere quoting would leak into JSON keys.
        cfg.setProperty("hibernate.globally_quoted_identifiers", String.valueOf(store.equals("h2")));
        cfg.setProperty("hibernate.auto_quote_keyword", "true");
        cfg.setProperty("hibernate.physical_naming_strategy",
                "org.hibernate.boot.model.naming.CamelCaseToUnderscoresNamingStrategy");
        cfg.setProperty("hibernate.jdbc.time_zone", "UTC");
        cfg.setStatementInspector(sql -> {
            LAST_SQL.set(sql);
            return sql;
        });
        return cfg.buildSessionFactory();
    }
}
