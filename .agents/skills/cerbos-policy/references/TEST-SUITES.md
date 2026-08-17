# Test Suite Files (`*_test.yaml`)

Declarative test files that `cerbos compile` runs as part of validation. For interactive debugging of a single condition, see [TESTING.md](TESTING.md).

## File Structure

Test files MUST end in `_test.yaml`. The schema is strict — only documented fields are allowed (`additionalProperties: false`).

Every test file MUST begin with the test suite language-server header:

```yaml
# yaml-language-server: $schema=https://api.cerbos.dev/latest/cerbos/policy/v1/TestSuite.schema.json
```

```yaml
# yaml-language-server: $schema=https://api.cerbos.dev/latest/cerbos/policy/v1/TestSuite.schema.json
name: "DocumentPolicyTest"
description: "Tests for document resource policy"  # optional

principals:
  owner_user:
    id: "user1"
    roles:
      - "user"
    attr:
      department: "engineering"
  other_user:
    id: "user2"
    roles:
      - "user"
    attr:
      department: "sales"

resources:
  owned_doc:
    kind: "document"
    id: "doc1"
    attr:
      owner: "user1"
      public: false

# Optional: auxData fixtures — only needed if policies use request.auxData (e.g., JWT claims).
# Skip this section entirely if your policies don't reference auxiliary data.
# See "AuxData Fixtures" below for the jwt / jwts forms.

tests:
  - name: "Owner can edit their document"
    input:
      principals:
        - owner_user
      resources:
        - owned_doc
      actions:
        - edit
        - view
    expected:
      - principal: owner_user
        resource: owned_doc
        actions:
          edit: EFFECT_ALLOW
          view: EFFECT_ALLOW

  - name: "Non-owner cannot edit"
    input:
      principals:
        - other_user
      resources:
        - owned_doc
      actions:
        - edit
    expected:
      - principal: other_user
        resource: owned_doc
        actions:
          edit: EFFECT_DENY
```

## Strict Schema — Allowed Fields Only

Root-level fields (ONLY these):

- `name` (required), `description`, `options`, `skip`, `skipReason`
- `principals`, `principalGroups`, `resources`, `resourceGroups`, `auxData`
- `tests` (required)

Each test case in `tests` (ONLY these):

- `name` (required): string
- `input` (required): object
- `expected` (required): array
- `description`, `options`, `skip`, `skipReason`: optional

`input` object (ONLY these):

- `actions` (required): array of strings
- `principals`: array of fixture key strings
- `resources`: array of fixture key strings
- `principalGroups`, `resourceGroups`: arrays (optional)
- `auxData`: STRING reference to fixture key (NOT an object)

`expected` array items (ONLY these):

- `actions` (required): object mapping action name to `"EFFECT_ALLOW"` or `"EFFECT_DENY"`
- `principal`: string (fixture key)
- `resource`: string (fixture key)
- `principals`, `resources`, `principalGroups`, `resourceGroups`: arrays (alternative to singular)
- `outputs`: array (optional, for output assertions)

Every list above is exhaustive: the schema sets `additionalProperties: false`, so any field outside them fails validation. Assertions describe the expected *effect* of a request — the policy's own vocabulary (conditions, roles, rules) has no place in a test file.

## `options` Block

Valid at suite root and per test case; the test-case block overrides the suite block. Allowed keys (ONLY these):

| Key | Type | Purpose |
|---|---|---|
| `now` | RFC3339 timestamp string | Pins what `now()` returns. Use this for any time-dependent rule — never rely on the wall clock |
| `strictEvaluation` | boolean | Treat runtime CEL errors as terminal, denying the affected action (v0.55+) |
| `defaultPolicyVersion` | string | Policy version for fixtures that do not set one |
| `defaultScope` | string | Scope for fixtures that do not set one |
| `lenientScopeSearch` | boolean | Fall back to ancestor scopes when an exact scope has no policy |
| `globals` | object | Values bound to `globals.*` in expressions |

```yaml
options:
  now: "2026-01-15T10:00:00Z"
  strictEvaluation: true
  globals:
    tenant_tier: "enterprise"
```

Pinning `now` is the correct way to test time-based conditions — see [TESTING.md](TESTING.md).

## AuxData Fixtures

Two mutually exclusive forms. Single token uses `jwt` with claims inline; multiple named tokens use `jwts` with a mandatory `claims` level (v0.55+).

```yaml
auxData:
  # Single JWT → request.auxData.jwt.dept
  valid_jwt:
    jwt:
      iss: my.domain
      aud: ["x", "y"]
      dept: engineering

  # Named JWTs → request.auxData.jwts.primary.claims.dept
  multi_token:
    jwts:
      primary:
        claims:
          iss: my.domain
          dept: engineering
      delegated:
        claims:
          act: service-account
```

Reference a fixture from a test with `input.auxData: valid_jwt` — a STRING key, never an inline object.

## Output Assertions

When a rule has an `output` block, assert on it under `expected[].outputs`:

```yaml
expected:
  - principal: owner_user
    resource: owned_doc
    actions:
      view: EFFECT_ALLOW
    outputs:
      - action: view
        expected:
          - src: resource.document.vdefault#view-rule
            val:
              key1: value1
```

Each entry under `expected` accepts `src`, `val`, `error`, and `action`. Since v0.54, an output expression that fails at runtime reports its `error` in the response and the test failure shows the actual evaluation error — so a mismatch tells you whether the expression errored or merely produced the wrong value. An `output` block with an empty expression is a compile error.

## Shared Fixtures

- There is only ONE `principals.yaml` and ONE `resources.yaml` per `testdata/` folder
- Fixtures are SHARED across ALL `*_test.yaml` files in the parent directory
- When writing fixtures, combine all principals/resources needed by all tests in that domain into single files

## Standalone Fixture File Format

Standalone fixture files MUST have a top-level key AND the matching language-server header:

- Principal fixtures: `$schema=https://api.cerbos.dev/latest/cerbos/policy/v1/TestFixture/Principals.schema.json`
- Resource fixtures: `$schema=https://api.cerbos.dev/latest/cerbos/policy/v1/TestFixture/Resources.schema.json`
- AuxData fixtures: `$schema=https://api.cerbos.dev/latest/cerbos/policy/v1/TestFixture/AuxData.schema.json`

```yaml
# testdata/principals.yaml
# yaml-language-server: $schema=https://api.cerbos.dev/latest/cerbos/policy/v1/TestFixture/Principals.schema.json
principals:
  owner_user:
    id: "user1"
    roles:
      - "user"
    attr:
      department: "engineering"
```

```yaml
# testdata/resources.yaml
# yaml-language-server: $schema=https://api.cerbos.dev/latest/cerbos/policy/v1/TestFixture/Resources.schema.json
resources:
  owned_doc:
    kind: "document"
    id: "doc1"
    attr:
      owner: "user1"
      public: false
```

## Coverage Guidelines

- Every policy needs tests for BOTH ALLOW and DENY cases
- If a rule has a condition, cover both the condition-true and condition-false branches
- If a derived role depends on `P.attr.context.*`, populate that field in the principal fixture — otherwise the derived role silently never applies and tests flip to DENY
- Every DENY rule with a condition needs a test proving it actually fires. A DENY whose condition errors at runtime silently no-ops and the action gets allowed — the strict pass catches it ([CEL.md](CEL.md#strict-evaluation-v055))
- Pin `options.now` for any time-dependent rule rather than relying on the wall clock

## Common Test Failures

| Symptom | Cause | Fix |
|---|---|---|
| `additional property not allowed` | Extra field in test or fixture | Remove the field — schema is strict |
| Expected ALLOW, got DENY | Derived role not matching, or fixture missing an attribute | Reproduce in REPL ([TESTING.md](TESTING.md)) |
| Expected DENY, got ALLOW | Duplicate unconditional rule, wildcard action grant, or a DENY condition erroring at runtime | Search for conflicting rules; re-run with `--strict-evaluation` |
| Passes normally, fails under `--strict-evaluation` | Condition errors at runtime and silently evaluates false | Fix the expression or add the missing fixture attribute |
| Output assertion fails with an `error` value | Output expression itself errored rather than returning a wrong value | Fix the output expression, not the expected `val` |
| Time-dependent test flaky | Rule uses `now()` with no pinned clock | Set `options.now` |
| "0 tests executed" | No `*_test.yaml` files present | Expected — not an error unless you expected tests |
