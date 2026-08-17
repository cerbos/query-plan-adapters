# CEL, Conditions, and Common Errors

## Special Objects

- `P` (Principal): `P.id`, `P.roles`, `P.attr.*`
- `R` (Resource): `R.id`, `R.kind`, `R.attr.*`
- `V` (exported variables): `V.is_owner`, etc.
- `request.auxData.jwt.CLAIM`: claims from a single JWT
- `request.auxData.jwts.NAME.claims.CLAIM`: claims from a named JWT (v0.55+, multiple tokens). The `claims` segment is mandatory. Bracket form: `request.auxData.jwts["NAME"].claims["CLAIM"]`
- `runtime.effectiveDerivedRoles`: derived roles that matched for this request

`jwt` and `jwts` are mutually exclusive on a request — pick one form and use it consistently across policies and fixtures.

## Common CEL Patterns

```cel
R.attr.owner == P.id                              # Ownership
"admin" in P.roles                                # Role check
P.id in R.attr.members                            # List membership
R.attr.dept == P.attr.dept                        # Attribute match
R.attr.environment == "production"                # Direct comparison (prefer this)
```

Prefer direct attribute comparisons over defensive `has()` guards. Avoid patterns like `has(R.attr.environment) && R.attr.environment == "production"` — the guard adds complexity without value when the schema marks the attribute required. Use `has()` when the attribute is genuinely optional: under strict evaluation (see below) an unguarded read of an absent field is a terminal error that denies the action, so optional attributes must be guarded rather than left to evaluate falsey.

The rule of thumb: let the schema decide. Required attribute → compare directly. Optional attribute → guard with `has()`.

## Function Catalogue

Cerbos ships far more than the CEL standard library, so an unfamiliar function name is usually a real one. Check this table and confirm in the REPL ([TESTING.md](TESTING.md)) before concluding a function does not exist. Entries marked **0.55+** require Cerbos v0.55.0 or later.

| Group | Functions |
|---|---|
| Core | `has()`, `size()`, `type()`, `in`, ternary `? :` |
| Lists | `all()`, `exists()`, `exists_one()`, `filter()`, `map()`, `sort()`, `sortBy()`, `reverse()`, `distinct()`, `flatten()`, `slice()`, `lists.range()` |
| Sets (legacy) | `except()`, `hasIntersection()`, `intersect()`, `isSubset()` |
| Sets **0.55+** | `sets.contains()`, `sets.equivalent()`, `sets.intersects()` |
| Strings | `matches()`, `contains()`, `startsWith()`, `endsWith()`, `indexOf()`, `lastIndexOf()`, `charAt()`, `replace()`, `split()`, `join()`, `substring()`, `trim()`, `upperAscii()`, `lowerAscii()`, `format()` |
| Regex **0.55+** | `regex.extractAll(str, pattern)`, `regex.replace(str, pattern, replacement, [limit])` |
| Encoding | `base64.encode()`, `base64.decode()`, `json.encode()` **0.55+** |
| Time | `now()`, `timestamp()`, `duration()`, `timeSince()`, plus `getFullYear()`/`getMonth()`/`getDayOfWeek()`/`getHours()`/… accessors (most take an optional timezone) |
| Math | `math.abs()`, `math.sign()`, `math.round()`, `math.ceil()`, `math.floor()`, `math.trunc()`, `math.greatest()`, `math.least()`, `math.bitAnd()`/`bitOr()`/`bitXor()`/`bitNot()`, `math.bitShiftLeft()`/`bitShiftRight()`, `math.isFinite()`/`isInf()`/`isNaN()` |
| Hierarchy | `hierarchy()`, `.ancestorOf()`, `.descendentOf()`, `.immediateParentOf()`, `.immediateChildOf()`, `.siblingOf()`, `.overlaps()`, `.commonAncestors()` |
| IP / CIDR **0.55+** | `ip()`, `isIP()`, `cidr()`, `isCIDR()`, `ip.isCanonical()`; IP methods `.family()`, `.isLoopback()`, `.isUnspecified()`, `.isGlobalUnicast()`, `.isLinkLocalUnicast()`, `.isLinkLocalMulticast()`; CIDR methods `.containsIP()`, `.containsCIDR()`, `.isMask()`, `.masked()`, `.prefixLength()`. Supersedes the legacy `inIPAddrRange()` |
| Paths | `basePath()`, `dirPath()`, `extPath()`, `joinPath()`, `relPath()`, `pathHasPrefix()`, `pathMatch()`, `pathMatchAnyOf()` |
| SPIFFE | `spiffeID()`, `spiffeTrustDomain()`, `.trustDomain()`, `.path()`, `.isMemberOf()`, `spiffeMatchExact()`, `spiffeMatchOneOf()`, `spiffeMatchTrustDomain()`, `spiffeMatchAny()` |

Prefer `sets.contains` / `sets.equivalent` / `sets.intersects` over the legacy `isSubset` / `hasIntersection` family for new policies — the legacy functions had inconsistent behaviour when comparing mixed numeric types (int vs double), fixed in v0.55.

## Strict Evaluation (v0.55+)

By default, a runtime error in a condition — accessing a field that does not exist, comparing mismatched types — evaluates to **false**. On an `EFFECT_DENY` rule that is a security hole: the deny silently no-ops and a lower-priority ALLOW wins.

```yaml
# The DENY below never fires if R.attr.nonexistent is absent from the resource.
# Under default evaluation the principal is ALLOWED.
- actions: ["view"]
  effect: EFFECT_DENY
  roles: ["user"]
  condition:
    match:
      expr: R.attr.nonexistent == "blocked"
- actions: ["view"]
  effect: EFFECT_ALLOW
  roles: ["user"]
```

Strict evaluation makes such errors terminal — the affected action is denied instead. It is **opt-in and off by default**, at three levels:

| Where | How |
|---|---|
| Test run | `cerbos compile --strict-evaluation <dir>` |
| Single suite or test case | `options: { strictEvaluation: true }` — see [TEST-SUITES.md](TEST-SUITES.md) |
| Deployed PDP | `engine.strictEvaluation: true` in the server config |

The denial propagates: an errored rule condition denies the actions that rule covers; an errored variable denies every action whose condition references it; an errored derived-role condition denies actions referencing that derived role or `runtime.effectiveDerivedRoles`.

A failure under `--strict-evaluation` that passes without it means a condition is erroring at runtime — a latent bug regardless of which mode production runs in. Fix the expression or the fixture.

Runtime CEL errors are logged by the PDP (`engine.celErrorLogLevel`, default `warn`) and recorded in audit entries, so these bugs are discoverable in a running deployment too.

## Condition Nesting (preferred over inline CEL)

Use structured condition blocks instead of complex inline CEL for readability.

### Single expression

```yaml
condition:
  match:
    expr: R.attr.owner == P.id
```

### `all` (AND)

```yaml
condition:
  match:
    all:
      of:
        - expr: R.attr.status == "DRAFT"
        - expr: R.attr.dept == P.attr.dept
        - expr: P.attr.level >= 3
```

### `any` (OR)

```yaml
condition:
  match:
    any:
      of:
        - expr: R.attr.owner == P.id
        - expr: P.id in R.attr.editors
        - expr: R.attr.public == true
```

### `none` (NOT)

```yaml
condition:
  match:
    none:
      of:
        - expr: R.attr.archived == true
        - expr: R.attr.deleted == true
```

### Nested operators

```yaml
condition:
  match:
    all:
      of:
        - expr: R.attr.status == "DRAFT"
        - any:
            of:
              - expr: R.attr.owner == P.id
              - expr: P.id in R.attr.editors
        - none:
            of:
              - expr: R.attr.archived == true
```

For quoted strings in expressions, use YAML block scalar syntax:

```yaml
expr: >
  "GB" in R.attr.geographies
```

## Naming Conventions

| Element | Convention | Example |
|---|---|---|
| Resource | singular lowercase | `document`, `expense_report` |
| Action | lowercase verb or `verb_noun` | `view`, `approve`, `view_comments` |
| Derived role | snake_case, descriptive | `document_owner`, `same_department` |
| Exported variable | snake_case with `is_`/`has_`/`can_` prefix | `is_owner`, `has_approval` |
| Role policy file | snake_case role name | `release_manager.yaml` |

## Exported Variables Extraction Rule

Extract a condition to `common_vars.yaml` **only if it is reused 2+ times across policies**. Single-use conditions stay inline. Only `import` variables in files that actually reference `V.*`.

## Common Pitfalls

These are the most frequent and silent policy bugs. Check for them before validating.

### 1. `roles` + `derivedRoles` on the same rule = OR, not AND

```yaml
# WRONG — matches anyone who is a "user" OR the owner
- actions: ["edit"]
  roles: ["user"]
  derivedRoles: ["owner"]
  effect: EFFECT_ALLOW
```

To require BOTH, express the role check inside the derived role:

```yaml
# CORRECT
derivedRoles:
  definitions:
    - name: user_owner
      parentRoles: ["user"]
      condition:
        match:
          expr: R.attr.owner == P.id
```

### 2. Wildcard DENY overrides specific ALLOWs

```yaml
# WRONG — the DENY wins
- actions: ["*"]
  roles: ["guest"]
  effect: EFFECT_DENY
- actions: ["view"]
  roles: ["guest"]
  effect: EFFECT_ALLOW
```

Use an explicit action list on the DENY, or rely on the default deny.

### 3. Conditional actions must not also appear unconditionally

If `reopen` has a condition in one rule for a given role, it must not appear in any unconditional rule for that same role on the same resource — the unconditional rule wins and the condition is silently bypassed.

### 4. Derived role depending on `P.attr.context.*`

If a derived role checks `has(P.attr.context.case_id)`, every test principal fixture must populate `attr.context.case_id`, otherwise the derived role silently never applies and the test flips to DENY.

### 5. DENY rule whose condition errors at runtime

The most dangerous silent failure. A `EFFECT_DENY` condition that touches a missing attribute or mismatched type evaluates to false by default, the deny never fires, and an ALLOW takes over. Tests pass. See [Strict Evaluation](#strict-evaluation-v055) — validate with `--strict-evaluation` to catch it.

### 6. Regex-identifiable syntax mistakes

| Wrong | Correct |
|---|---|
| `match.all: [...]` | `match.all.of: [...]` (also `any.of`, `none.of`) |
| `P.role == "admin"` | `"admin" in P.roles` |
| `request.aux_data.jwt.sub` | `request.auxData.jwt.sub` |
| `request.auxData.jwts.NAME.sub` | `request.auxData.jwts.NAME.claims.sub` (`claims` is mandatory) |
| Inline double-quoted CEL without block scalar | `expr: >` block scalar |

## Error Fix Table

Look the symptom up here; SKILL.md Phase 4 carries the order to work through them in.

| Error | Common cause | Fix |
|---|---|---|
| CEL syntax error | Unescaped quotes in inline `expr` | Use block scalar `expr: >` |
| Unknown CEL function | Typo, or a v0.55+ function on an older PDP | Check the [function catalogue](#function-catalogue); confirm the PDP version before rewriting |
| Invalid timestamp / regex literal | Malformed constant in an expression | Caught at compile time since v0.55 — fix the literal; the line and column are in the error |
| `additional property not allowed` in test | Extra field on test schema | Remove the field — test schema is strict |
| Empty output expression | `output` block with no expression | Compile error since v0.54 — supply an expression or drop the block |
| Derived role never matches | `parentRoles` missing the principal's role | Add the role, or fix the principal fixture |
| Action unexpectedly allowed | Duplicate unconditional rule, or a DENY whose condition errors | Search for another rule granting the action; re-run with `--strict-evaluation` |
| Action unexpectedly denied | Wildcard DENY earlier in the file | Narrow the DENY or reorder |
| Passes normally, fails under `--strict-evaluation` | A condition errors at runtime and silently evaluates false | Real bug — fix the expression or add the missing fixture attribute |
| `import` of unknown derived role | File not loaded or name mismatch | Check `name:` in the derived_roles file |

Since v0.54, YAML and policy errors carry a line and column number — read the position before searching the file by hand.
