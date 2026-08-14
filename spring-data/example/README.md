# cerbos-spring-data — multi-resource enterprise example

A Spring Boot + JPA application that exercises the `cerbos-spring-data` adapter through a
real `JpaSpecificationExecutor`, H2 database, and Cerbos PDP. It authorizes three independent
resource types—`photo`, `album`, and `workspace`—and doubles as an end-to-end edge-case harness.
Every authorization-bearing request obtains a resource-specific `PlanResources` result,
translates it to a typed JPA `Specification`, and executes the resulting SQL.

Policy filtering is not reimplemented in Java. The one intentional application-owned predicate
is the mandatory tenant boundary, composed outside the Cerbos specification so it still applies
to `KIND_ALWAYS_ALLOWED`. Changing the resource policies under [`policies/`](policies/) changes
the policy-controlled result sets without changing the app.

This directory holds **two programs sharing one Gradle build**. The photo-sharing application is
the larger of the two and the one this README is mostly about. Beside it,
`dev.cerbos.example.demo` is a no-argument program that exercises the repository's
[shared demo domain](../../demo) — the five usage shapes every adapter's example implements —
and prints one JSON document for `demo/scripts/run-example.sh` to diff. See
[The shared demo domain](#the-shared-demo-domain) below. The two have separate policies,
separate schemas, and separate PDPs; nothing about the photo-sharing app changed to make room
for the second, because the demo domain is a floor rather than a ceiling
([ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)).

> [!WARNING]
> **Demo identity only — do not copy this pattern.** The endpoints accept `user`, `role`,
> `tenant`, and `groups` as unauthenticated query parameters so the smoke harness can switch
> principals from `curl`. In a real application, **derive the principal from your
> authentication layer (Spring Security, a verified JWT/OIDC token, mTLS identity), never
> from request input**. Anything a caller can type is not an identity: here
> `?role=admin` grants the unconditional-ALLOW admin rule and `?tenant=...` crosses the
> tenant boundary. The correct shape is to build `Principal.newInstance(...)` from
> authenticated, server-controlled state (e.g. `SecurityContextHolder` /
> `Authentication`) and never read identity or roles from parameters, headers, or bodies.

## What it covers

| Resource kind | Persistence/auth path | Representative rules |
|---|---|---|
| `photo` | `PhotoRepository` + `PhotoService` + `photo.yaml` | ownership, tags, labels, tenant-safe grants |
| `album` | `AlbumRepository` + `AlbumService` + `album.yaml` | owner, shared flag, collaborator membership |
| `workspace` | `WorkspaceRepository` + `WorkspaceService` + `workspace.yaml` | active state, owner, member collection |

Every kind has its own entity, repository, attribute map, Cerbos resource policy, endpoint,
fixtures, and PDP audit assertions. The sample does not emulate multiple kinds with a discriminator
column or reuse a cast `Specification<?>`.

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

The seed set deliberately includes two tenants, nullable grant subjects, a cross-tenant malformed
grant, duplicate qualifying grants, empty collections, nested dimensions, and SQL `LIKE`
metacharacters.

## Request flow

1. `GET /photos`, `/albums`, or `/workspaces` supplies a demo principal, tenant, and action.
2. The resource-specific service calls the live PDP with `Resource.newInstance("photo")`,
   `"album"`, or `"workspace"`.
3. `SpringDataQueryPlanAdapter` maps the plan using that entity's independent attribute map.
4. The service composes `tenantBoundary AND authorizationPlan` plus any local filter.
5. The matching typed `JpaSpecificationExecutor` executes the Criteria query against H2.
6. The controller returns only the rows allowed by the composed specification.

## Layout

```text
example/
├── policies/photo.yaml
├── policies/album.yaml
├── policies/workspace.yaml
├── cerbos-config.yaml
├── docker-compose.yml           # the photo-sharing app's PDP, on 23592/23593
├── settings.gradle.kts          # no composite build — see "How this example resolves the adapter"
├── build.gradle.kts             # Spring Boot 3.5, Spring Data JPA, H2, and the demoJar task
├── run.sh                       # the spring-data half of demo/scripts/run-example.sh
├── scripts/smoke.sh             # live PDP + Boot + HTTP assertions
├── scripts/smoke-edge-cases.sh  # full-stack regression tripwire (see "Edge-case regression scenarios")
└── src/main/
    ├── resources/application.yaml
    ├── resources/application-demo.yaml  # the demo-domain program's datasource
    ├── java/dev/cerbos/example/CerbosClientConfig.java  # the PDP client, shared by both programs
    ├── java/dev/cerbos/example/demo/
    │   ├── DemoApplication.java     # no-argument program, one JSON document on stdout
    │   ├── DemoShapes.java          # the five shared usage shapes
    │   ├── DemoDocument.java        # the demo domain's one entity
    │   ├── DemoDocumentRepository.java
    │   └── DemoSeeds.java           # ../../demo/seeds.json, parsed
    └── java/dev/cerbos/example/photos/
        ├── Photo.java            # tenant-scoped aggregate root and its relations
        ├── PhotoDetails.java     # embedded dimensions
        ├── PhotoLabel.java       # structured one-to-many relation
        ├── PhotoGrant.java       # nullable direct/group delegated-access rows
        ├── PhotoRepository.java  # JpaRepository + JpaSpecificationExecutor
        ├── PhotoService.java     # plan, mapping, Specification execution
        ├── PhotoController.java  # list and paginated REST endpoints
        ├── Album.java            # independently authorized album entity
        ├── AlbumRepository.java
        ├── AlbumService.java     # Resource.newInstance("album")
        ├── AlbumController.java  # GET /albums
        ├── Workspace.java        # independently authorized workspace entity
        ├── WorkspaceRepository.java
        ├── WorkspaceService.java # Resource.newInstance("workspace")
        ├── WorkspaceController.java # GET /workspaces
        ├── AccessContext.java    # shared principal/tenant construction
        ├── SeedData.java         # nine adversarial photos across two tenants + edge-regression seeds
        └── PhotosApplication.java
```

## How this example resolves the adapter

`dev.cerbos:cerbos-spring-data` is resolved from **mavenLocal as a real Maven coordinate**, so
`gradle -p .. publishToMavenLocal` is a prerequisite of everything here. `run.sh`, `smoke.sh` and
`smoke-edge-cases.sh` each do it for themselves, so any one of them is still a single command.

It used to be a Gradle composite build (`includeBuild("..")`), which substitutes the adapter's
source tree for the declared coordinate and therefore resolves neither its POM nor its Gradle
module metadata. That is half of what an example exists to prove
([ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md)), and the class of bug it
hid is not hypothetical: `cerbos-sdk-java` declares protobuf at runtime-only scope in its own
module metadata. Two things this example now depends on are consequences of resolving the real
thing:

- it declares `cerbos-sdk-java` itself, because the adapter publishes the SDK at **runtime**
  scope — correct, since a consumer that never names an SDK type should not compile against one,
  and this application does name them;
- it declares **no** `protobuf-java` version, because the adapter publishes one at runtime scope
  pinned to the gencode the SDK was generated against. gRPC drags in older protobuf-java versions
  transitively and an older runtime throws `ProtobufRuntimeVersionException` at first message
  decode, so restating the version here would make the example pass whether or not the adapter
  still declares it.

The `example/` directory is a separate Gradle build rather than a source set of the adapter, so
it cannot reach the published jar. `run.sh` asserts that rather than leaving it to inspection.

## Run it

Prerequisites: Docker, curl, jq, Gradle 8.x, and JDK 17+.

`CERBOS_HOST` has **no default**. Cerbos's own 3592/3593 are the ports every adapter's
`cerbos run` test sidecar binds, so a default would not fail on a mistake — it would quietly plan
against the wrong policy suite. This example's PDP is published on 23592/23593 instead, and the
demo domain's on 13592/13593.

```bash
# terminal 1: install the adapter, then start the live Cerbos PDP
gradle -p .. publishToMavenLocal --no-daemon
docker compose up -d

# terminal 2: Spring Boot app
CERBOS_HOST=localhost:23593 gradle bootRun --no-daemon

# terminal 3: queries
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

Or run the complete harness:

```bash
./scripts/smoke.sh
```

### SQL logging

`src/main/resources/application.yaml` ships with the Hibernate SQL loggers commented out:

```yaml
# org.hibernate.SQL: DEBUG
# org.hibernate.orm.jdbc.bind: TRACE
```

Uncomment them locally to watch the SQL each authorization plan turns into. Keep them off
anywhere logs are collected: the Cerbos planner constant-folds **principal** attributes
(emails, departments, owner ids — potentially PII) into the plan constants, the adapter
binds those constants as JDBC parameters, and `org.hibernate.orm.jdbc.bind: TRACE` prints
every bind value verbatim on every authorized query (e.g.
`binding parameter [1] as [VARCHAR] - [alice@corp.com]`). The adapter library itself logs
nothing — this is purely a logging-configuration concern.

The smoke script starts the PDP and app, checks the full scenario matrix, validates both
pages and totals for a relation-based paginated query, and checks invalid page bounds return
400. After each authorization-bearing HTTP assertion, it parses Cerbos's own JSON audit logs
and requires exactly one `PlanResources` access and decision pair—with the same call ID, the
expected action, and a query-plan filter in the PDP response. It also compares the full action
and resource-kind multiset as a summary and proves controller-rejected requests do not reach
the PDP. The run fails unless the observed kind set is exactly `album`, `photo`, and `workspace`.

## The shared demo domain

The second program in this build, `dev.cerbos.example.demo`, exercises the repository's
[demo domain](../../demo): one resource kind, four flat scalar attributes, three actions, shared
verbatim by every adapter's example. It takes no arguments, prints one JSON document to
stdout, and is run by the shared runner:

```bash
demo/scripts/run-example.sh spring-data   # from the repository root
```

The runner starts the demo PDP, invokes [`run.sh`](run.sh), and diffs the emitted document
against `demo/expected.json`. `run.sh` publishes the adapter, builds the program's own executable
jar via the `demoJar` task, and launches it with `java -jar` — Gradle is kept out of the launching
process because its stdout carries lifecycle output, and the contract is one JSON document on
stdout with everything else on stderr. `DemoApplication` does the other half by redirecting
`System.out` to stderr before Spring starts and writing the document through the handle it kept.

The five shapes it implements are the ones defined in
[cerbos/query-plan-adapters#349](https://github.com/cerbos/query-plan-adapters/issues/349):

| Shape | What it exercises here |
|---|---|
| Plain filtered list | `findAll(Specification)` on a `KIND_CONDITIONAL` plan |
| `KIND_ALWAYS_ALLOWED` | `Specification.unrestricted()` — every row, no `WHERE` clause |
| `KIND_ALWAYS_DENIED` | the always-false predicate — no rows |
| Pagination | `findAll(Specification, Pageable)`, whose separate COUNT query rebuilds the predicate against a second `Root` |
| Composition | the adapter's `Specification` ANDed with one the application owns |

Composition is the shape that earns the exercise, and it is a single `.and(...)` for all three
plan kinds because every `toSpecification` overload returns a `Specification` covering all of
them — the caller never switches on the kind. Note what it is *not*: this program does not prove
what the adapter translates. `AdversarialConformanceTest` does that, against a hostile corpus
with a live PDP as the oracle, on H2, PostgreSQL and MySQL. What no conformance harness can cover
is the published package surface (every harness compiles against the adapter's own source set)
and usage shapes past a single flat query.

Why the photo-sharing application stays: the demo domain is thin by construction — roughly the
intersection of every adapter's query language, one of which is a vector store — and
[ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md) makes it a floor
rather than a ceiling. Everything in this README above and below this section is coverage nothing
shared asserts, including six historical bug fixes pinned by
[`scripts/smoke-edge-cases.sh`](scripts/smoke-edge-cases.sh).

## Adapter mapping

The mapping is intentionally not always one-to-one. It demonstrates dotted embedded paths
and maps policy-facing label names to different Java property names:

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

Each request also sends `interests` as a Cerbos principal attribute. The `similar` action
uses it in `hasIntersection(resource.tags, principal.interests)`, which the planner reduces
to values the adapter can apply to the tag relation.

## Enterprise isolation and delegation

Every repository query starts with an application-owned `tenantId` specification. It is ANDed
outside the adapter result, so even an unconditional admin plan cannot escape the selected
tenant. In production, the tenant must come from authenticated server context—not a query
parameter. The optional `minRating` predicate demonstrates a second local specification without
placing it outside the tenant fence by mistake.

The `delegated-view` policy evaluates structured grant rows. A grant must match the photo's
tenant, carry the `view` permission, and target either the principal ID or one of the principal's
tenant-qualified groups. Fixtures include direct and group grants, two matching rows for one
photo, the same group slug in another tenant, a wrong-permission grant, and a deliberately
malformed grant whose tenant disagrees with its parent.

The `group-grant`, `no-group-grant`, and `no-group-grant-safe` actions pin a subtle security
property: a null child attribute evaluates UNKNOWN, not false. A null-only collection is excluded
under both the positive and unguarded negated policy; explicitly checking `groupId != null`
changes the negated result in a visible, documented way.

## Album and workspace authorization

Albums and workspaces are not thin routes over `Photo`. Each has a separate table, entity,
`JpaSpecificationExecutor`, service, Cerbos attribute map, and resource policy:

- `album` exercises scalar ownership/shared-state predicates and membership in an
  `@ElementCollection` of collaborators.
- `workspace` combines a mandatory active-state predicate with owner-or-member access and a
  separate owner-only administration action.

Both types retain the application-owned tenant fence around conditional, always-allowed, and
always-denied plans. Their fixtures include Acme and Globex rows so the smoke matrix proves an
unconditional admin plan remains tenant-limited for all three persistence models.

## Why the paginated endpoint matters

`JpaSpecificationExecutor.findAll(spec, pageable)` evaluates the specification for both the
content query and a count query. `/photos/page` confirms that a conditional plan containing
correlated label and grant subqueries can be rebuilt for both Criteria roots and produce stable
IDs, `totalElements`, and `totalPages` without duplicate parent rows. Matching fixtures contain
duplicate qualifying children specifically to catch accidental join-based duplication.

## Full-cycle verification

Matching response IDs alone could pass if application code accidentally replaced the PDP with
local filtering. The smoke harness therefore verifies both ends of every successful scenario:

1. The HTTP response contains the IDs produced by executing the translated Spring Data
   specification against H2.
2. A Cerbos access record confirms the `/PlanResources` RPC reached the PDP.
3. A decision record with the same `callId` contains the expected
   `planResources.input.resource.kind`, `planResources.input.actions`, and a non-null
   `planResources.output.filter.kind`.

Each HTTP assertion checks its own audit delta, so repeated actions cannot hide a missing or
duplicate call. The final 42-entry resource/action multiset is a second summary check, and the
observed resource-kind set must be exactly the three intended kinds. Readiness uses an
unmapped route and creates no PDP traffic; after the rejected pagination/filter requests, a valid
`audit-sentinel` request acts as an audit flush barrier and the complete delta must contain only
that sentinel.

## Edge-case regression scenarios

`./scripts/smoke-edge-cases.sh` (run in CI after `smoke.sh`) is a full-stack regression
tripwire rather than a tutorial. Each scenario re-creates, end to end — Spring Boot app,
live PDP, real `JpaSpecificationExecutor` against H2 — a high-severity adapter bug that has
been fixed on `main`. Every assertion passes today and would have failed (wrong row set, or
an HTTP 500) immediately before the corresponding fix, so a regression in any of these code
paths breaks this harness even if unit-level coverage is ever weakened.

The scenarios are deliberately quarantined from the pedagogical demo: the `edge-*` actions
live in a clearly-delimited section at the bottom of [`policies/photo.yaml`](policies/photo.yaml),
their fixtures (`e1`–`e6`) sit in an isolated `edge` tenant so no `smoke.sh` expectation
changes, and some expressions (`0.0 / 0.0`, `size(title) > 4294967296`) are intentionally
pathological probes — not policy-writing guidance.

| Scenario | Pins | Historical wrong behavior |
|---|---|---|
| `edge-ieee-eq` / `edge-ieee-ne` | [#274](https://github.com/cerbos/query-plan-adapters/pull/274) | `eq`/`ne` over `field + constant` solved algebraically (`0.1 - 0.7`); IEEE addition does not invert, so a PDP-denied row (`score = -0.6`) was included by `eq` and excluded by `ne` |
| `edge-nan-ordering` | [#275](https://github.com/cerbos/query-plan-adapters/pull/275) | constant `NaN` ordering used `Double.compare`'s total order, so `NaN > 0.5` was true and non-public rows leaked through the ternary's else-arm |
| `edge-retention` | [#279](https://github.com/cerbos/query-plan-adapters/pull/279) | `timestamp(createdAt) < now() - duration("24h")` — the most common compliance shape — threw for every query (each request a 500) instead of comparing the temporal column |
| `edge-bracket-title` | [#285](https://github.com/cerbos/query-plan-adapters/pull/285) | LIKE escaping missed SQL Server's `[...]` character class; `startsWith("[SEC]")` matched rows starting with S/E/C and missed literal `[SEC]…` rows |
| `edge-size-huge` | [#286](https://github.com/cerbos/query-plan-adapters/pull/286) | `size(title) > 4294967296` truncated the threshold with an `(int)` cast to `0`, returning every non-empty title instead of no rows |
| `DELETE /photos/bulk-unsafe` | [#273](https://github.com/cerbos/query-plan-adapters/pull/273) | `delete(Specification)` with a Relation-mapped predicate deleted 0 photos while silently destroying their collection rows; the adapter now throws before any SQL runs — the endpoint surfaces the guard as HTTP 409 and the harness then proves all rows survived |

`DELETE /photos/bulk-unsafe` is an intentionally-failing demonstration endpoint: a 409 with
the guard's message is its correct, asserted behavior. The safe deletion pattern is
`findAll(spec)` followed by `deleteAllById(ids)` — see the `SpringDataQueryPlanAdapter` Javadoc.

### Dialect-sensitive findings

Two further fixed findings only manifest on real MySQL/SQL Server and cannot be reproduced
on this example's H2 database, so they are deliberately not contorted into this harness:

- **Collation-blind string matching** ([#272](https://github.com/cerbos/query-plan-adapters/pull/272)):
  case-/accent-insensitive default collations make SQL match rows the PDP denies. Covered by
  the adapter's real-database CI legs (`test-database` in `.github/workflows/spring-data.yaml`),
  which run the differential `check()` oracle on PostgreSQL and MySQL with case-sensitive
  schema collation; see "Database collation requirements" in the adapter README.
- **MySQL decimal-literal arithmetic** ([#284](https://github.com/cerbos/query-plan-adapters/pull/284)):
  Connector/J's client-side prepared statements evaluated the adapter's IEEE double
  arithmetic in exact decimal. The MySQL CI leg runs in both prepared-statement modes to pin
  the fix; see "MySQL: keeping arithmetic IEEE-faithful" in the adapter README.
