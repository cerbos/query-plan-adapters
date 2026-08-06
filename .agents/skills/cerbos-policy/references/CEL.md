# CEL, Conditions, and Common Errors

## Special Objects

- `P` (Principal): `P.id`, `P.roles`, `P.attr.*`
- `R` (Resource): `R.id`, `R.kind`, `R.attr.*`
- `V` (exported variables): `V.is_owner`, etc.
- `request.auxData.*`: auxiliary data such as JWT claims

## Common CEL Patterns

```cel
R.attr.owner == P.id                              # Ownership
"admin" in P.roles                                # Role check
P.id in R.attr.members                            # List membership
R.attr.dept == P.attr.dept                        # Attribute match
R.attr.environment == "production"                # Direct comparison (prefer this)
```

Prefer direct attribute comparisons over defensive `has()` guards. Avoid patterns like `has(R.attr.environment) && R.attr.environment == "production"` — the guard adds complexity without value when schemas enforce attribute presence. Only use `has()` when the attribute is genuinely optional AND the policy must handle its absence.

Valid CEL functions: `has()`, `size()`, `matches()`, `now()`, `timestamp()`, `hierarchy()`.

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

### 5. Regex-identifiable syntax mistakes

| Wrong | Correct |
|---|---|
| `match.all: [...]` | `match.all.of: [...]` (also `any.of`, `none.of`) |
| `P.role == "admin"` | `"admin" in P.roles` |
| `request.aux_data.jwt.sub` | `request.auxData.jwt.sub` |
| Inline double-quoted CEL without block scalar | `expr: >` block scalar |

## Error Priority and Fix Table

Fix in this order. Do not skip ahead — a lower-priority error often disappears once a higher one is fixed.

1. YAML parse errors
2. CEL syntax errors
3. Schema validation errors (`additionalProperties: false`)
4. Compile errors (unresolved imports, missing derived roles, unknown variables)
5. Test failures

| Error | Common cause | Fix |
|---|---|---|
| CEL syntax error | Unescaped quotes in inline `expr` | Use block scalar `expr: >` |
| Unknown CEL function | Typo | Use `has()`, `size()`, `matches()`, `now()`, `timestamp()`, `hierarchy()` |
| `additional property not allowed` in test | Extra field on test schema | Remove the field — test schema is strict |
| Derived role never matches | `parentRoles` missing the principal's role | Add the role, or fix the principal fixture |
| Action unexpectedly allowed | Duplicate unconditional rule | Search for another rule granting the same action |
| Action unexpectedly denied | Wildcard DENY earlier in the file | Narrow the DENY or reorder |
| `import` of unknown derived role | File not loaded or name mismatch | Check `name:` in the derived_roles file |

When a condition fails in a non-obvious way, reproduce it in `cerbosctl repl` — see [TESTING.md](TESTING.md) — before patching.
