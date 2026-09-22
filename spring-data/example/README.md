# cerbos-spring-data — multi-resource enterprise example

Two Spring Boot + JPA programs that use `cerbos-spring-data` through real `JpaSpecificationExecutor`
repositories, an H2 database and a live Cerbos PDP. Both share one Gradle build:

- **`dev.cerbos.example.demo`** — the repository's [shared demo domain](../../demo): the five usage
  shapes every adapter's example implements, printed as one JSON document.
- **`dev.cerbos.example.photos`** — a photo-sharing app authorizing three resource kinds (`photo`,
  `album`, `workspace`), which doubles as an end-to-end edge-case harness.

They have separate policies, schemas and PDPs.

> [!WARNING]
> **Demo identity only — do not copy this pattern.** The photo endpoints take `user`, `role`,
> `tenant` and `groups` as unauthenticated query parameters so the smoke harness can switch
> principals from `curl`: `?role=admin` grants the unconditional-ALLOW admin rule and `?tenant=...`
> crosses the tenant boundary. In a real application, build `Principal.newInstance(...)` from your
> authentication layer (Spring Security `Authentication`, a verified JWT/OIDC token, mTLS) — never
> from parameters, headers or bodies.

## Run it

Prerequisites: Docker (with Compose), curl, jq, Gradle 8.x and JDK 17+.

**Demo domain** — from the repository root:

```bash
demo/scripts/run-example.sh spring-data
```

The runner starts the demo PDP (ports 13592/13593), calls [`run.sh`](run.sh), and diffs its output
against `demo/expected.json`. `run.sh` publishes the adapter to mavenLocal, checks the adapter jar
contains no example classes, builds the `demoJar`, and runs it with `java -jar` so stdout carries
only the JSON document (`DemoApplication` also redirects `System.out` to stderr before Spring
starts).

**Photo-sharing app** — from `spring-data/example/`:

```bash
./scripts/smoke.sh              # full scenario matrix + PDP audit-log checks
./scripts/smoke-edge-cases.sh   # regression tripwire (see "Edge-case regression scenarios")
```

Or by hand:

```bash
# terminal 1: install the adapter, start this example's PDP (ports 23592/23593)
gradle -p .. publishToMavenLocal --no-daemon
docker compose up -d

# terminal 2: the app on :8080
CERBOS_HOST=localhost:23593 gradle bootRun --no-daemon

# terminal 3
curl -s "http://localhost:8080/photos?user=alice&action=view" | jq '[.[].id]'
curl -s "http://localhost:8080/photos?user=alice&action=similar&interests=travel,food" | jq '[.[].id]'
curl -s "http://localhost:8080/photos?user=alice&action=delegated-view&groups=finance,engineering" | jq '[.[].id]'
curl -s "http://localhost:8080/photos?user=admin&role=admin&tenant=globex&action=view" | jq '[.[].id]'
curl -s "http://localhost:8080/photos?user=alice&action=view&minRating=5" | jq '[.[].id]'
curl -s "http://localhost:8080/photos?user=alice&action=needs-moderation" | jq '[.[].id]'
curl -s "http://localhost:8080/photos/page?user=alice&action=needs-moderation&page=0&size=1" |
  jq '{ids: [.content[].id], totalElements, totalPages}'
curl -s "http://localhost:8080/albums?user=alice&action=view" | jq '[.[].id]'
curl -s "http://localhost:8080/workspaces?user=alice&action=access" | jq '[.[].id]'
```

`CERBOS_HOST` has **no default**, so a mistake fails instead of silently planning against another
local PDP on Cerbos's default 3592/3593. Both programs give each PDP call a 30-second deadline.

### SQL logging

`src/main/resources/application.yaml` ships with the Hibernate SQL loggers commented out:

```yaml
# org.hibernate.SQL: DEBUG
# org.hibernate.orm.jdbc.bind: TRACE
```

Uncomment them locally to see each plan's SQL. **Never enable them where logs are collected:** the
planner folds principal attributes (emails, departments, owner ids — potentially PII) into plan
constants, and `bind: TRACE` prints every bound value (e.g.
`binding parameter [1] as [VARCHAR] - [alice@corp.com]`). The adapter itself logs nothing.

## The shared demo domain

One resource kind, four flat scalar attributes, three actions, shared verbatim by every adapter's
example. The five shapes ([#349](https://github.com/cerbos/query-plan-adapters/issues/349)):

| Shape | What it exercises here |
|---|---|
| Plain filtered list | `findAll(Specification)` on a `KIND_CONDITIONAL` plan |
| `KIND_ALWAYS_ALLOWED` | `Specification.unrestricted()` — every row, no `WHERE` clause |
| `KIND_ALWAYS_DENIED` | the always-false predicate — no rows |
| Pagination | `findAll(Specification, Pageable)`, whose COUNT query rebuilds the predicate against a second `Root` |
| Composition | the adapter's `Specification` ANDed with one the application owns — one `.and(...)` for every plan kind |

This program proves packaging and usage, not translation semantics — `AdversarialConformanceTest`
does that against the hostile corpus on H2, PostgreSQL and MySQL. The demo domain is a floor, not a
ceiling ([ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)); the photo app
covers what it doesn't.

## What the photo app covers

Each kind has its own entity, repository, attribute map, policy, endpoint, fixtures and audit
assertions:

| Resource kind | Persistence/auth path | Representative rules |
|---|---|---|
| `photo` | `PhotoRepository` + `PhotoService` + `photo.yaml` | ownership, tags, labels, tenant-safe grants |
| `album` | `AlbumRepository` + `AlbumService` + `album.yaml` | owner, shared flag, collaborator `@ElementCollection` |
| `workspace` | `WorkspaceRepository` + `WorkspaceService` + `workspace.yaml` | active state, owner, member collection, owner-only admin action |

| Scenario | Policy/operator | JPA shape |
|---|---|---|
| Public, archived, ownership rules | `and`, `or`, `eq`, bare booleans | Scalar columns |
| Optional location | `!= null` | Nullable scalar / `IS NOT NULL` |
| Discovery thresholds | `>=` | Integer scalar plus dotted `@Embedded` path |
| Flat tags | `in`, `hasIntersection` | `@ElementCollection<String>` |
| Principal interests | principal list attribute | Runtime value substituted into a relation predicate |
| Tenant isolation | local `tenant AND plan AND filter` composition | Mandatory scalar fence around every plan kind |
| Delegated access | nested `exists`, direct user or group | Structured `@OneToMany` grants |
| Grant integrity | child tenant equals outer resource tenant | Field-to-field comparison inside a lambda |
| Nullable grant subjects | positive, negated, and null-guarded `exists` | SQL/CEL three-valued logic |
| Duplicate matching grants | two qualifying children for one photo | Correlated subquery without duplicate roots |
| Optional rating filter | local `Specification.and(...)` | Application filter composed outside authorization |
| Moderation labels | `exists` with nested `and` | Correlated subquery over `@OneToMany` entities |
| Review state | `all`, `exists_one` | Collection macro semantics, including empty collections |
| Missing labels | `size(...) == 0` | `NOT EXISTS` collection shortcut |
| `%` and `_` in titles | `contains` | Escaped SQL `LIKE` literals |
| Admin | unconditional allow | `KIND_ALWAYS_ALLOWED` |
| Unknown `publish` action | no matching rule | `KIND_ALWAYS_DENIED` |
| Relation-heavy pages | label and delegated-grant predicates | Separate content/count queries with stable totals |
| Full request cycle | PDP audit-log assertion | Matched call IDs, resource kinds, actions, and filters |

Request flow: `GET /photos`, `/albums` or `/workspaces` → the service plans with
`Resource.newInstance("<kind>")` → `SpringDataQueryPlanAdapter` translates with that entity's
attribute map → the service composes `tenantBoundary AND plan` (plus any local filter) → the
repository runs it against H2.

Notable details:

- **Tenant fence.** Every query starts with an application-owned `tenantId` Specification, ANDed
  outside the adapter's result, so even an admin's `KIND_ALWAYS_ALLOWED` plan stays in its tenant.
  It is the only policy-like filter in Java; changing [`policies/`](policies/) changes result sets
  without changing the app. In production the tenant must come from authenticated server context.
- **Mapping.** Not always one-to-one — dotted embedded paths and renamed label fields:

  ```java
  Map.entry("request.resource.attr.metadata.width",
          AttributeMapping.field("details.pixelWidth")),
  Map.entry("request.resource.attr.tags",
          AttributeMapping.relation("tags")),
  Map.entry("request.resource.attr.labels",
          AttributeMapping.relation("labels", Map.of(
                  "name", AttributeMapping.field("labelName"),
                  "confidence", AttributeMapping.field("confidence"),
                  "reviewed", AttributeMapping.field("reviewed")))),
  Map.entry("request.resource.attr.grants",
          AttributeMapping.relation("grants", Map.of(
                  "tenantId", AttributeMapping.field("tenantId"),
                  "permission", AttributeMapping.field("permission"),
                  "userId", AttributeMapping.field("userId"),
                  "groupId", AttributeMapping.field("groupId"))))
  ```

  The `similar` action uses the principal attribute `interests` in
  `hasIntersection(resource.tags, principal.interests)`, which the planner reduces to values.
- **Delegation.** `delegated-view` requires a grant matching the photo's tenant, the `view`
  permission, and the principal's id or one of its tenant-qualified groups. Fixtures include direct
  and group grants, duplicate matches, the same group slug in another tenant, a wrong-permission
  grant and a grant whose tenant disagrees with its parent. `group-grant`, `no-group-grant` and
  `no-group-grant-safe` pin that a null child attribute is UNKNOWN, not false.
- **Pagination.** `/photos/page` checks that a plan with correlated label and grant subqueries is
  rebuilt for both the content and count roots with stable ids, `totalElements` and `totalPages`
  and no duplicate parents.

### Full-cycle verification

`smoke.sh` verifies both ends of every successful scenario, so local filtering could not pass in
place of the PDP:

1. The response ids come from the translated Specification executed against H2.
2. A Cerbos access record shows the `/PlanResources` RPC reached the PDP.
3. A decision record with the same `callId` has the expected resource kind, actions and a non-null
   `planResources.output.filter.kind`.

Each assertion checks its own audit delta; the run compares the full resource/action multiset,
requires the kind set to be exactly `album`, `photo` and `workspace`, and proves controller-rejected
requests (invalid page bounds return 400) never reach the PDP. An `audit-sentinel` request acts as
the audit flush barrier.

## Edge-case regression scenarios

`./scripts/smoke-edge-cases.sh` (run in CI after `smoke.sh`) re-creates, end to end, high-severity
adapter bugs fixed on `main`. Each assertion would have failed (wrong rows, or an HTTP 500) before its
fix. The `edge-*` actions sit at the bottom of [`policies/photo.yaml`](policies/photo.yaml) and their
fixtures (`e1`–`e6`) in an isolated `edge` tenant, so `smoke.sh` is unaffected. Some expressions
(`0.0 / 0.0`, `size(title) > 4294967296`) are pathological probes, not policy guidance.

| Scenario | Pins | Historical wrong behavior |
|---|---|---|
| `edge-ieee-eq` / `edge-ieee-ne` | [#274](https://github.com/cerbos/query-plan-adapters/pull/274) | `eq`/`ne` over `field + constant` solved algebraically; IEEE addition doesn't invert, so a PDP-denied row (`score = -0.6`) was included by `eq` and excluded by `ne` |
| `edge-nan-ordering` | [#275](https://github.com/cerbos/query-plan-adapters/pull/275) | `NaN` ordering used `Double.compare`'s total order, so `NaN > 0.5` was true and non-public rows leaked through a ternary's else-arm |
| `edge-retention` | [#279](https://github.com/cerbos/query-plan-adapters/pull/279) | `timestamp(createdAt) < now() - duration("24h")` threw on every query (HTTP 500) |
| `edge-bracket-title` | [#285](https://github.com/cerbos/query-plan-adapters/pull/285) | LIKE escaping missed SQL Server's `[...]` class; `startsWith("[SEC]")` matched rows starting with S/E/C |
| `edge-size-huge` | [#286](https://github.com/cerbos/query-plan-adapters/pull/286) | `size(title) > 4294967296` was truncated to `0` by an `(int)` cast, returning every non-empty title |
| `DELETE /photos/bulk-unsafe` | [#273](https://github.com/cerbos/query-plan-adapters/pull/273) | `delete(Specification)` with a relation predicate deleted 0 photos but destroyed their collection rows; the adapter now throws first — the endpoint returns 409 and the harness proves every row survived |

`DELETE /photos/bulk-unsafe` is meant to fail: a 409 with the guard's message is its asserted
behaviour. The safe pattern is `findAll(spec)` then `deleteAllById(ids)`.

### Dialect-sensitive findings

Two fixed findings only show on real MySQL/SQL Server, not H2, so they are covered by the adapter's
`test-database` CI legs (`.github/workflows/spring-data.yaml`) instead:

- **Collation-blind string matching** ([#272](https://github.com/cerbos/query-plan-adapters/pull/272))
  — see "Database collation requirements" in the adapter README.
- **MySQL decimal-literal arithmetic** ([#284](https://github.com/cerbos/query-plan-adapters/pull/284))
  — see "MySQL: keeping arithmetic IEEE-faithful" in the adapter README.

## How this example resolves the adapter

`dev.cerbos:cerbos-spring-data` resolves from **mavenLocal as a real Maven coordinate**, so its POM
and Gradle module metadata are exercised ([ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md)).
`run.sh`, `smoke.sh` and `smoke-edge-cases.sh` each run `gradle -p .. publishToMavenLocal`
themselves. It is not a composite build (`includeBuild("..")`), which would substitute the source
tree and skip the published metadata. Two consequences:

- The example declares `cerbos-sdk-java` itself, because the adapter publishes the SDK at
  **runtime** scope and this app names SDK types.
- It declares **no** `protobuf-java` version: the adapter publishes one at runtime scope matching
  the SDK's gencode, and restating it here would hide a regression (an older transitive protobuf
  from gRPC throws `ProtobufRuntimeVersionException`).

`example/` is a separate Gradle build, not a source set of the adapter, so it can't leak into the
published jar; `run.sh` asserts that.

## Layout

```text
example/
├── policies/{photo,album,workspace}.yaml
├── cerbos-config.yaml
├── docker-compose.yml           # the photo app's PDP, on 23592/23593
├── settings.gradle.kts          # no composite build
├── build.gradle.kts             # Spring Boot 3.5, Spring Data JPA, H2, and the demoJar task
├── run.sh                       # the spring-data half of demo/scripts/run-example.sh
├── scripts/smoke.sh             # live PDP + Boot + HTTP assertions
├── scripts/smoke-edge-cases.sh  # regression tripwire
└── src/main/
    ├── resources/application.yaml
    ├── resources/application-demo.yaml   # the demo program's datasource
    ├── java/dev/cerbos/example/CerbosClientConfig.java  # PDP client, shared
    ├── java/dev/cerbos/example/demo/     # DemoApplication, DemoShapes, DemoDocument(+Repository), DemoSeeds
    └── java/dev/cerbos/example/photos/   # Photo/Album/Workspace entities, repositories, services,
                                          # controllers, AccessContext, SeedData
```
