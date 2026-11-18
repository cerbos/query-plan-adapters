# Domain stress results, 2026-11-18

Sixteen adapters, ten domains, every store leg the adapter's CI runs, 368 goldens per leg (184 per
PDP, 0.57.0 and 0.56.0). Each adapter has a hand-written mapping per domain; its `friction.md`
says what each mapping took.

## Over-grants an adapter emits

None of the 0.57.0 goldens over-grants anywhere except exposed on MySQL. Three more adapter
over-grants need a slightly different policy or row than the goldens hold, and each was
reproduced against the real store.

1. **exposed, MySQL: membership in a list held in a correlated join.** legal `lawyer_ali view`
   returns d08 and d09 (team member `L-ALI`) on both PDPs. The emitted
   `col = 'v' AND col = (CONVERT('v' USING utf8mb4) COLLATE utf8mb4_0900_bin)` relies on the
   binary half, which MySQL 8.4 drops when `col` is part of the join table's primary key it looks
   up by, so `ai_ci` decides. jpa collates the column side and passes. Repro in
   `exposed/friction.md`.
2. **drizzle: two relations to the same table share one unaliased name.** `origin.country ==
   destination.country` compares the inner `hubs.country` with itself and returns every shipment
   (7 of 9 over-granted). The real policy uses `!=`, so the golden only shows an under-grant.
3. **drizzle: a self-relation subquery is uncorrelated.** `manager.id == "e02"` emits
   `exists (select 1 from employees where employees.id = employees.manager_id and employees.id = ?)`.
   Once any row manages itself (a common way to mark the top of a tree), every row matches.
4. **Mappings a user could plausibly write that over-grant.** Each adapter has a correct
   alternative, and the domain mappings use it:
   - mongoose: a `Map` field declared as a to-one relation gets no missing-key guard on `!=`.
   - sequel: a SQLite `json` column mapped as a field makes `size()` the string length
     (`'[]'` has length 2).
   - drizzle: a JSON column with no mapper entry turns `"k" in labels` into `labels in ('"k"')`.
   - jpa: Hibernate `globally_quoted_identifiers` quotes JSON keys (`labels->>'"tier"'`), so every
     key reads NULL and negated tests pass.

## Planner findings (every adapter agrees, so they are in the plan)

- **0.56.0 drops `has()` conjuncts.** Both over-grants every adapter shows (healthcare
  `patient_p3 view` returns n13, hr `mgr_e02 view_compensation` returns e05) come from this, plus
  9 to 13 under-grants per leg. 0.57.0 keeps `has()` and those cases pass.
- **0.57.0 still mis-plans three shapes (under-grants, fail closed):**
  - `R.attr.privilege == null || R.attr.privilege == ""` (explicit nulls) is planned as
    `privilege == null` alone, so the `""` row (legal d06) is lost.
  - `!(has(R.attr.patient.vip) && R.attr.patient.vip)` is planned as `!vip`, so a row with no
    `vip` is lost (healthcare nurse_ana).
  - A DENY rule `has(R.attr.labels.restricted) && R.attr.labels.restricted == "yes"` loses its
    `has()`; how that lands depends on the adapter's missing-key semantics (iot member_acme).

## Adapter bugs that fail closed

- prisma: 14 where-inputs Prisma itself rejects: a to-one hop at the start of an `exists` path
  rendered as `some`, the `hub` hop inside a lambda dropped, a `not: null` guard on a required list
  element, and `in` against a JSON column rendered as equality.
- sqlalchemy: `has()` on a timestamp column is refused as a bare temporal read (16 per leg).
- drizzle: a to-many reached through two inferred to-one hops throws unless given a placeholder
  default field; `has()` over a transform mapping throws a plain `Error`.
- ent, activerecord, sequel, mongodb-ruby, sqlalchemy: `"k" in <json map>` silently becomes UNKNOWN
  (no rows) instead of a refusal.
- prisma, drizzle, sqlalchemy, jpa, sequel: a call-level explicit-null setting does not reach `!=`
  on attributes that do not declare it (activerecord fixed this in #592).
- efcore: `AttributeMap.Field` rejects `decimal` although the README says it converts it.
- mongoose: with references instead of embedded documents, unmapped paths throw a plain `Error`,
  not the refusal type.

## Verdicts per leg

| adapter | store | pass | refused | error | under | over | 0.57.0 pass / over |
|---|---|---|---|---|---|---|---|
| activerecord | mariadb | 337 | 12 | 0 | 17 | 2 | 175 / 0 |
| activerecord | mysql | 337 | 12 | 0 | 17 | 2 | 175 / 0 |
| activerecord | postgres | 337 | 12 | 0 | 17 | 2 | 175 / 0 |
| activerecord | sqlite | 337 | 12 | 0 | 17 | 2 | 175 / 0 |
| convex | convex | 349 | 0 | 0 | 17 | 2 | 181 / 0 |
| drizzle | mysql | 327 | 12 | 1 | 27 | 1 | 168 / 0 |
| drizzle | postgres | 327 | 12 | 1 | 27 | 1 | 168 / 0 |
| drizzle | sqlite | 331 | 8 | 1 | 27 | 1 | 170 / 0 |
| efcore | postgres | 341 | 8 | 0 | 17 | 2 | 177 / 0 |
| efcore | sqlite | 341 | 8 | 0 | 17 | 2 | 177 / 0 |
| efcore | sqlserver | 341 | 8 | 0 | 17 | 2 | 177 / 0 |
| elasticsearch-java | elasticsearch-8.19.25 | 287 | 69 | 0 | 11 | 1 | 148 / 0 |
| elasticsearch-java | elasticsearch-9.5.6 | 287 | 69 | 0 | 11 | 1 | 148 / 0 |
| ent | mysql | 337 | 12 | 0 | 17 | 2 | 175 / 0 |
| ent | postgres | 337 | 12 | 0 | 17 | 2 | 175 / 0 |
| ent | sqlite | 337 | 12 | 0 | 17 | 2 | 175 / 0 |
| exposed | h2 | 337 | 14 | 0 | 15 | 2 | 175 / 0 |
| exposed | mysql | 335 | 14 | 0 | 15 | 4 | 174 / 1 |
| exposed | postgres | 337 | 14 | 0 | 15 | 2 | 175 / 0 |
| exposed | sqlite | 337 | 14 | 0 | 15 | 2 | 175 / 0 |
| jpa | h2 | 331 | 20 | 0 | 15 | 2 | 172 / 0 |
| jpa | mysql | 331 | 20 | 0 | 15 | 2 | 172 / 0 |
| jpa | postgres | 335 | 14 | 0 | 17 | 2 | 174 / 0 |
| langchain-chromadb | chroma-where-only | 260 | 101 | 0 | 6 | 1 | 128 / 0 |
| langchain-chromadb | chroma | 345 | 4 | 0 | 17 | 2 | 179 / 0 |
| mongodb-ruby | mongo-7.0 | 347 | 0 | 0 | 19 | 2 | 180 / 0 |
| mongodb-ruby | mongo-9.0 | 347 | 0 | 0 | 19 | 2 | 180 / 0 |
| mongoose | mongo-7 | 349 | 0 | 0 | 17 | 2 | 181 / 0 |
| mongoose | mongo-9 | 349 | 0 | 0 | 17 | 2 | 181 / 0 |
| pgx | postgres | 337 | 14 | 0 | 15 | 2 | 175 / 0 |
| prisma | mysql-v6 | 305 | 32 | 14 | 15 | 2 | 158 / 0 |
| prisma | mysql-v7 | 305 | 32 | 14 | 15 | 2 | 158 / 0 |
| prisma | postgres-v6 | 305 | 32 | 14 | 15 | 2 | 158 / 0 |
| prisma | postgres-v7 | 305 | 32 | 14 | 15 | 2 | 158 / 0 |
| prisma | sqlite-v6 | 305 | 32 | 14 | 15 | 2 | 158 / 0 |
| prisma | sqlite-v7 | 305 | 32 | 14 | 15 | 2 | 158 / 0 |
| sequel | mysql | 333 | 18 | 0 | 15 | 2 | 172 / 0 |
| sequel | postgres | 333 | 18 | 0 | 15 | 2 | 172 / 0 |
| sequel | sqlite | 333 | 18 | 0 | 15 | 2 | 172 / 0 |
| spring-data | h2 | 331 | 20 | 0 | 15 | 2 | 172 / 0 |
| sqlalchemy | mysql+schema | 323 | 28 | 0 | 15 | 2 | 160 / 0 |
| sqlalchemy | mysql | 327 | 24 | 0 | 15 | 2 | 162 / 0 |
| sqlalchemy | postgresql+schema | 323 | 28 | 0 | 15 | 2 | 160 / 0 |
| sqlalchemy | postgresql | 327 | 24 | 0 | 15 | 2 | 162 / 0 |
| sqlalchemy | sqlite+schema | 323 | 28 | 0 | 15 | 2 | 160 / 0 |
| sqlalchemy | sqlite | 327 | 24 | 0 | 15 | 2 | 162 / 0 |

## Mapping friction (1 easy, 5 hard)

| adapter | health | fintech | logistics | education | legal | retail | hr | media | iot | gov |
|---|---|---|---|---|---|---|---|---|---|---|
| prisma | 2 | 3 | 3 | 4 | 2 | 1 | 2 | 3 | 5 | 3 |
| drizzle | 2 | 2 | 3 | 4 | 2 | 1 | 4 | 3 | 5 | 3 |
| sqlalchemy | 1 | 1 | 2 | 1 | 3 | 1 | 2 | 4 | 4 | 2 |
| ent | 2 | 2 | 3 | 1 | 3 | 1 | 1 | 3 | 4 | 2 |
| pgx | 3 | 3 | 3 | 3 | 3 | 2 | 2 | 3 | 4 | 2 |
| activerecord | 2 | 2 | 2 | 3 | 3 | 1 | 2 | 4 | 5 | 2 |
| sequel | 2 | 2 | 2 | 4 | 3 | 1 | 2 | 4 | 5 | 3 |
| jpa | 2 | 2 | 2 | 4 | 3 | 1 | 1 | 3 | 5 | 2 |
| exposed | 1 | 1 | 2 | 1 | 1 | 1 | 2 | 4 | 5 | 1 |
| efcore | 2 | 3 | 4 | 2 | 2 | 1 | 2 | 3 | 4 | 2 |
| elasticsearch-java | 2 | 2 | 2 | 2 | 4 | 3 | 1 | 3 | 3 | 3 |
| mongoose | 2 | 1 | 1 | 1 | 2 | 1 | 1 | 1 | 4 | 1 |
| mongodb-ruby | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 3 | 1 |
| convex | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 |
| langchain-chromadb | 3 | 3 | 3 | 3 | 4 | 4 | 2 | 2 | 3 | 4 |

spring-data runs the jpa mappings unchanged and matches jpa/h2 on every golden.

The common hard spots: a JSON object column read as a map (iot) has no mapping form in most SQL
adapters; a native `text[]` (media) needs a hand-written relation over `unnest`/`json_each`; a
list reached through to-one hops (education) needs every hop declared in jpa, activerecord, sequel
and drizzle; and explicit-null domains need every nullable attribute declared again.
