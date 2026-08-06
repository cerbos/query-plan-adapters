# Testing Cerbos Policies

Two layers of testing:

1. **`cerbosctl repl`** — interactive CEL evaluation. Use this when debugging a single condition, exploring attribute shapes, or figuring out why a rule isn't matching.
2. **`*_test.yaml` suites** — declarative regression tests compiled and run by `cerbos compile`. Every REPL finding should be codified here.

Rule of thumb: reach for the REPL the moment a condition behaves unexpectedly. Do not patch blindly.

## Running the REPL

```bash
docker run --rm -it -v "$(pwd):/policies" -w /policies \
  ghcr.io/cerbos/cerbosctl:latest repl
```

Mount the policy directory so `:load` can read files. Exit with `:quit`.

Native install (if `cerbosctl` is on PATH): `cerbosctl repl`.

## REPL Primitives

| Command | Purpose |
|---|---|
| `:let P = {...}` | Set the principal |
| `:let R = {...}` | Set the resource |
| `:let V = {...}` | Set exported variables |
| `:let request = {...}` | Set the full request object |
| `<expression>` | Evaluate a raw CEL expression against the current scope |
| `:load path/to/policy.yaml` | Load a policy into the session |
| `:rules` | List rules from the loaded policy |
| `:exec #N` | Execute rule N against current `P`/`R` |
| `:vars` | Show all bindings in scope |
| `:help` | Full command list |

## Debugging Recipe

Work from the outside in. When a test produces an unexpected DENY:

1. **Reproduce the fixture in the REPL.** Copy the principal and resource from `testdata/*.yaml` into `:let P = ...` and `:let R = ...`.
2. **Evaluate the condition in isolation.** Paste the raw CEL expression. If it returns `false`, the problem is the expression or the fixture — not the rule wiring.
3. **Inspect attributes.** `P.roles`, `P.attr`, `R.attr` — verify every field the condition touches actually exists and has the expected type.
4. **Check each AND branch separately.** For `all.of` conditions, evaluate each sub-expression individually to find which one fails.
5. **Load the policy and `:exec` the rule.** Confirms the rule is wired up as expected and that derived roles resolve.

## Common Failure Modes

### Derived role silently does not match

Symptom: rule expects a derived role like `owner` but the test gets DENY.

```
-> :let P = {"id":"u1","roles":["user"],"attr":{}}
-> :let R = {"kind":"document","id":"d1","attr":{"owner":"u1"}}
-> R.attr.owner == P.id
_ = true
```

If the expression is true but the rule still denies, the derived role's `parentRoles` does not include any role the principal holds. Check `parentRoles` against `P.roles`.

### `has(P.attr.x)` returns false unexpectedly

`has()` is strict about the full path. `has(P.attr.context.case_id)` is false if `P.attr.context` itself is absent. Build the parent first:

```
-> has(P.attr.context)
-> has(P.attr.context) && has(P.attr.context.case_id)
```

If a derived role depends on `P.attr.context.*`, the test principal fixture MUST populate that context field, otherwise the derived role silently never applies.

### Time-based conditions

`now()` returns the Cerbos runtime clock. To reproduce a failing time condition, bind a fixed `now`:

```
-> :let now = timestamp("2026-01-15T10:00:00Z")
-> R.attr.expires_at > now
```

Codify the expected behaviour in a `*_test.yaml` with an explicit fixture timestamp — never rely on wall clock in tests.

### `R.attr` reference errors

`R.attr.owner` on a resource fixture without an `attr.owner` field is a runtime error, not `false`. Confirm the fixture before blaming the expression.

### `match.all` vs `match.all.of`

`match.all` with a direct list is invalid. It must be `match.all.of: [...]`. Same for `any` and `none`. Easy to catch in REPL: load the policy, and a syntax error surfaces immediately.

## REPL vs `*_test.yaml`

| Use REPL for | Use `*_test.yaml` for |
|---|---|
| Figuring out why a condition doesn't match | Permanent regression coverage |
| Exploring attribute shapes | ALLOW and DENY assertions |
| Sketching a new CEL expression | Enforcing the policy contract in CI |
| One-off "does this function exist" checks | Team-visible behaviour documentation |

Never stop at the REPL. Every confirmed behaviour belongs in a test file.

## Running the Full Test Suite

```bash
docker run --rm -v "$(pwd):/policies" \
  ghcr.io/cerbos/cerbos:latest compile /policies
```

`compile` runs both static validation and every `*_test.yaml` in the tree. Exit code 0 means policies compile and all tests pass.

If no `*_test.yaml` files exist, "0 tests executed" is expected — not an error. Only diagnose it if you expected tests to run.

## When a Test Fails

1. Read the failure line: it names the test case, principal, resource, and action.
2. Reproduce the exact principal/resource in the REPL.
3. Evaluate the condition that should have matched.
4. Fix the policy OR the fixture — whichever is wrong. Never delete a test to make validation pass.
5. Re-run `compile`. Do not batch multiple fixes before re-validating.
