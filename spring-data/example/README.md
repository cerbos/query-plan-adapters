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

> **Demo identity only:** the endpoints accept `user`, `role`, `tenant`, and `groups` query
> parameters so the smoke harness can switch principals. Never copy that identity handling into
> production. Derive all of them from authenticated, server-controlled state; otherwise a caller
> could request `role=admin` or cross a tenant boundary.

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
├── docker-compose.yml
├── settings.gradle.kts          # composite-build include of the adapter source
├── build.gradle.kts             # Spring Boot 3.5, Spring Data JPA, H2
├── scripts/smoke.sh             # live PDP + Boot + HTTP assertions
└── src/main/
    ├── resources/application.yaml
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
        ├── CerbosClientConfig.java
        ├── SeedData.java         # nine adversarial photos across two tenants
        └── PhotosApplication.java
```

## Run it

Prerequisites: Docker, curl, jq, Gradle 8.x, and JDK 17+.

```bash
# terminal 1: live Cerbos PDP
docker compose up -d

# terminal 2: Spring Boot app
gradle bootRun --no-daemon

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

The smoke script starts the PDP and app, checks the full scenario matrix, validates both
pages and totals for a relation-based paginated query, and checks invalid page bounds return
400. After each authorization-bearing HTTP assertion, it parses Cerbos's own JSON audit logs
and requires exactly one `PlanResources` access and decision pair—with the same call ID, the
expected action, and a query-plan filter in the PDP response. It also compares the full action
and resource-kind multiset as a summary and proves controller-rejected requests do not reach
the PDP. The run fails unless the observed kind set is exactly `album`, `photo`, and `workspace`.

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
