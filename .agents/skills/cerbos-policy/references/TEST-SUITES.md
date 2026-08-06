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

NEVER add fields like `condition`, `effect`, `roles`, `rule`, `match`, or any custom properties. Any extra field causes validation failure.

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
- Never delete a test to make validation pass; fix the policy or the fixture instead

## Common Test Failures

| Symptom | Cause | Fix |
|---|---|---|
| `additional property not allowed` | Extra field in test or fixture | Remove the field — schema is strict |
| Expected ALLOW, got DENY | Derived role not matching, or fixture missing an attribute | Reproduce in REPL ([TESTING.md](TESTING.md)) |
| Expected DENY, got ALLOW | Duplicate unconditional rule, or wildcard action grant | Search for conflicting rules |
| "0 tests executed" | No `*_test.yaml` files present | Expected — not an error unless you expected tests |
