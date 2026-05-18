# cerbos-spring-data — photo-sharing example

A minimal Spring Boot + JPA application that uses the [`cerbos-spring-data`](..) adapter
to filter a `photos` table according to a Cerbos `PlanResources` decision served by a real
PDP container.

## What it does

1. Spring Boot exposes `GET /photos?user=<id>&role=<role>&action=<action>`.
2. The controller calls the Cerbos PDP for a query plan over the `photo` resource.
3. The adapter turns the plan into a JPA `Specification<Photo>`.
4. `PhotoRepository.findAll(spec)` runs the SQL.

No filtering is hand-rolled in Java — the predicates come straight from the policy.

## Layout

```
example/
├── policies/photo.yaml          # resource policy (view/edit/delete/comment)
├── cerbos-config.yaml           # PDP config (audit logs to stdout)
├── docker-compose.yml           # spins up ghcr.io/cerbos/cerbos:latest
├── settings.gradle.kts          # composite-build include of ../ (the adapter)
├── build.gradle.kts             # Spring Boot 3.5 + JPA + H2 + adapter
├── scripts/smoke.sh             # end-to-end script (compose up → bootRun → curl asserts)
└── src/main/
    ├── resources/application.yaml
    └── java/dev/cerbos/example/photos/
        ├── PhotosApplication.java
        ├── Photo.java                  @Entity (id, ownerId, isPublic, isArchived, tags…)
        ├── PhotoRepository.java        JpaRepository + JpaSpecificationExecutor
        ├── PhotoService.java           builds plan, calls adapter, runs the spec
        ├── PhotoController.java        REST surface
        ├── CerbosClientConfig.java     @Bean CerbosBlockingClient
        └── SeedData.java               loads 6 photos at boot
```

## Policy at a glance

| Action  | Rule (role `user`)                                                          |
|---------|-----------------------------------------------------------------------------|
| view    | `(public && !archived) || ownerId == self`                                  |
| edit    | `ownerId == self`                                                           |
| delete  | `ownerId == self`                                                           |
| comment | `(public && !archived) || "friends" in tags || ownerId == self`             |

Role `admin` always allowed. See [`policies/photo.yaml`](policies/photo.yaml).

## Run it

```bash
# 1. start the Cerbos PDP (mounts ./policies into the container)
docker compose up -d

# 2. start the Spring Boot app
gradle bootRun --no-daemon
# … or with the wrapper from ../, if you have one

# 3. in another shell — hit the API
curl -s "http://localhost:8080/photos?user=alice&action=view" | jq '[.[].id]'
# => ["p1","p2","p5","p6"]
```

Or one-shot via the smoke test:

```bash
./scripts/smoke.sh
```

## End-to-end smoke run

`scripts/smoke.sh` brings up the PDP, runs `gradle bootRun` in the background, and asserts
the response IDs for eight `(user, role, action)` permutations. On success it prints the
last 20 lines of the PDP audit log so you can see actual `PlanResources` calls flowing
into the container — proving the result came from a live policy decision, not a stub.

## Adapter wiring

```java
private static final Map<String, AttributeMapping> PHOTO_ATTRS = Map.of(
    "request.resource.attr.ownerId",  AttributeMapping.field("ownerId"),
    "request.resource.attr.public",   AttributeMapping.field("isPublic"),
    "request.resource.attr.archived", AttributeMapping.field("isArchived"),
    "request.resource.attr.tags",     AttributeMapping.relation("tags")
);

PlanResourcesResult plan = cerbos.plan(
    Principal.newInstance(userId).withRoles(role),
    Resource.newInstance("photo"),
    action);

Result<Photo> result = SpringDataQueryPlanAdapter.toSpecification(plan, PHOTO_ATTRS);
return repository.findAll(result.toSpecification());
```

`AttributeMapping.relation("tags")` is what makes `"friends" in tags` translate to a
correlated `EXISTS` subquery against the `photo_tags` join table.

## What this proves

- The adapter compiles against a real Spring Boot 3.5 application without dragging in
  conflicting Spring/JPA versions (its Spring deps are `compileOnly`).
- The PDP — not the app — decides which photos are visible. Changing
  `policies/photo.yaml` and re-running the smoke script flips the result set without a
  single line of Java change.
- `KIND_ALWAYS_ALLOWED` (admin) and `KIND_CONDITIONAL` (user) both round-trip through
  the adapter and land as a usable `Specification<Photo>`.
