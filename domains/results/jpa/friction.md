# jpa: domain stress friction

Harness: `jpa/src/domainTest/java/dev/cerbos/queryplan/jpa/domains/` (`ADAPTER_DOMAIN_STORE=h2|postgres|mysql ./gradlew domainTest`,
a separate source set and task, so `./gradlew test` is unchanged). Each domain is one file: the `@Entity` classes
as static nested classes plus the `AttributeMapping` map. The shared SQL in `generated/sql/<dialect>.sql` loads each domain
into its own H2 database, PostgreSQL schema or MySQL database. Hibernate reads it through the entities with
`CamelCaseToUnderscoresNamingStrategy`. Identifiers are quoted globally on H2 only, plus `auto_quote_keyword`. Plans are decoded
with protobuf `JsonFormat` into `PlanResourcesResponse` and passed to `JpaQueryPlanAdapter.toFilter`. The query is
`select root.id where filter.toPredicate(...)` through the Criteria API. Stores: H2 2.5, PostgreSQL 16 (ICU en-US), and MySQL 8.4
(`utf8mb4_0900_ai_ci`). The ORM is Hibernate 6.6 (`baseline`).

## Results

| store | pass | refused | under | over | error |
|---|---|---|---|---|---|
| h2 | 331 | 20 | 15 | 2 | 0 |
| postgres | 335 | 14 | 17 | 2 | 0 |
| mysql | 331 | 20 | 15 | 2 | 0 |

Both over-grants on every store are PDP 0.56.0 plans that drop a `has()` (see below). jpa itself causes no over-grant.

## Per domain

Inference (naming strategy, `@ManyToOne` default join column, `mappedBy`) settles most columns. The mapping still has to
list every attribute path, because jpa has no inference from attribute name to path. Lines are the whole file:
entities plus mapping.

| domain | lines | configured beyond inference | not expressible | score |
|---|---|---|---|---|
| healthcare | 95 | `@Column(name="type")` for `noteType`, `is_deleted` for `deleted`; `@JoinColumn(note_id)` because the root PK is `note_id` (the default would be `note_note_id`); `@ElementCollection`+`@CollectionTable` for care team; `idAttribute=noteId`; `OMITTED` on nullable fields | none | 2 |
| fintech | 102 | `@JoinColumn(branch_code)` (natural key); counterparty flattened as three `field("cpX")` entries | none | 2 |
| logistics | 93 | two `@ManyToOne Hub` with explicit join columns; `trackingNumber`→`trackingNo`, `shipper`→`shipperAccount` | none (field-to-field timestamps not in this domain's plans) | 2 |
| education | 97 | **course staff is a to-many below two to-one hops.** `relation("assignment.course.staff")` is not accepted (`join()` takes one attribute), so the to-one hops `assignment` and `course` are declared as `relation(...)` with nested relations so the chain subquery reaches `staff`, while their scalars stay `field("assignment.course.term")` | `timestamp(a) > timestamp(b)` field-to-field (documented refusal) | 4 |
| legal | 93 | same to-one-as-relation trick for `matter.team` / `matter.screened`; **per-attribute `EXPLICIT`** on the 4 nullable columns as well as the call-level `EXPLICIT` | none | 3 |
| retail | 83 | `@Entity(name="CustomerOrder") @Table(name="order")` (reserved word, auto-quoted); `isGift`→`gift` | none | 1 |
| hr | 76 | `request.resource.id`→`id`; `@JoinColumn(location_code)`; self-relation is plain `field("manager.manager.id")` | none | 1 |
| media | 77 | `@ManyToMany @JoinTable` for contributors (maps naturally); `keywords` mapped as a field placeholder | `keywords` (`text[]` / JSON array): a relation must be a JPA association, so `in`/`exists` over it is `UnmappedAttributeException`; regex `^[a-z0-9]+(-[a-z0-9]+)*$` | 3 |
| iot | 127 | `labels` as a Hibernate JSON-aggregate `@Embeddable` (`@JdbcTypeCode(SqlTypes.JSON)`) with the three known keys, mapped `field("labels.env")` etc. Hibernate 6.6 supports this only on PostgreSQL here (H2/MySQL `AggregateSupportImpl` throws at boot), so a second entity graph with `labels` as opaque text is needed for H2/MySQL | `labels.*` and `"tier" in labels` on H2 and MySQL; free-form keys on every store (each key must be an embeddable field) | 5 |
| government | 83 | per-attribute `EXPLICIT` on the 4 nullable columns | regex with >256 alternatives | 2 |

**Hardest problem:** the JSON map column (iot). It works only through a Hibernate JSON aggregate on PostgreSQL. It also
carries a silent hazard, found while building the harness: with `hibernate.globally_quoted_identifiers=true`, Hibernate
quotes the JSON *keys* (`labels->>'"tier"'`), so every key reads NULL. `!("tier" in labels)` then held on every row and
over-granted (`admin_acme decommission` returned dv07, `tech_eu update_firmware` returned dv01). Global quoting is the
usual fix for H2's case-sensitive names. The harness now quotes only on H2.

## Notes beyond the verdicts

- **Call-level `EXPLICIT` is not null-safe for undeclared attributes.** With only
  `withNullAttributeRepresentation(EXPLICIT)`, `assignedTo != "sv-bo"` renders `assigned_to <> ?`. That is UNKNOWN for a NULL
  that `check()` sees as an explicit null, so government `supervisor_bo approve` under-granted k01. Declaring
  `field(..., EXPLICIT)` per attribute fixes it, and the mapping now does that. It fails closed. The README's line
  "Undeclared attributes follow the call-level option" reads as if the call-level option were enough. exposed's README states
  the asymmetry explicitly.
- **To-one hop declared as a relation (education, legal):** a `relation(...)` over a `@ManyToOne` turns a missing parent
  into an empty collection (FALSE) rather than UNKNOWN. So `!(x in parent.list)` would admit parentless rows. Both FKs are
  NOT NULL here, so this corpus does not exercise it. It is a hazard of the workaround, not a verdict.
- MySQL: the shared `government` SQL fails to load under `utf8mb4_0900_ai_ci` (agencies `DSS`/`dss` collide on the PK).
  The harness recreates that database with `utf8mb4_0900_bin`. This is a store limitation, not the adapter.
- Timestamps must be `Instant`, and the JVM runs in UTC: H2 and MySQL columns are zone-less.

## Non-pass verdicts by cause

**PDP 0.56.0 folds `has()` to true (plan is wrong; jpa is faithful to it). Every store, 0.56.0 only.**
- over-grant: healthcare `patient_p3 view` (n13), hr `mgr_e02 view_compensation` (e05)
- under-grant: education `prof_jones grade`, `ta_lee grade`; healthcare `admin_kim delete`, `dr_ortiz sign`,
  `researcher_li sign`; hr `ceo_e01 view`, `hr_admin terminate`, `mgr_e02 view`, `recruiter view`; media
  `reader_free read`, `reader_sub read`

Repro, hr `mgr_e02 view_compensation`. Policy: `R.attr.manager.id == P.id && R.attr.level < 6 && has(R.attr.salaryBand)`.
The 0.56.0 plan is `and(eq(R.manager.id,"e02"), lt(R.level,6))`, and the 0.57.0 plan adds `has(R.salaryBand)`. Row e05 has
`manager_id=e02, level=3, salary_band=NULL`, which is omitted in `check()` and so denied. Emitted query:
`select e.id from employees e left join employees m on m.id=e.manager_id where m.id=? and e.level<?` returns e05.
Healthcare is the same pattern: 0.56.0 drops `has(R.attr.signedAt)`, so n13 (`signed_at` NULL) is returned. The README's
gotcha "write `!= null` instead" covers it.

**The PDP plan drops a guard on both versions (plan narrower than the policy; under-grant, fails closed).**
- healthcare `nurse_ana view` (n02): `!(has(R.attr.patient.vip) && R.attr.patient.vip)` is planned as
  `not(R.patient.vip)`, so a patient with no `vip` is UNKNOWN in the plan but allowed by `check()`.
- legal `client_globex view` (d06): `R.attr.privilege == null || R.attr.privilege == ""` is planned as
  `eq(R.privilege, null)`. d06 has `privilege = ""`.
- iot `member_acme view` (PostgreSQL only; refused elsewhere): a DENY rule `has(labels.restricted) &&
  labels.restricted == "yes"` is planned as `not(eq(R.labels.restricted,"yes"))`, which is UNKNOWN for devices without the key.

**Documented refusal (`UnsupportedPlanShapeException`).**
- timestamp field-to-field: education `prof_jones/ta_lee flag_late`, government `auditor_cy audit`
- regex LIKE cannot spell: media `editor_pat publish`, government `foia_dee foia_release`

**Mapping the adapter cannot express (`UnmappedAttributeException`).**
- media `lawyer_quinn read`, `legal_review`: `keywords` is an array/JSON column, not an association
- iot `admin_acme decommission`, `member_acme view`, `tech_eu update_firmware` on H2 and MySQL: JSON map column (store/Hibernate
  limitation; PostgreSQL passes through the JSON aggregate)
