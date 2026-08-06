---
name: cerbos-policy
description: Generate Cerbos authorization policies from requirements, PDFs, or specifications. Use when creating access control policies, resource permissions, derived roles, or RBAC/ABAC rules. Triggers on "cerbos", "authorization policy", "access control", "RBAC", "ABAC".
license: Apache-2.0
compatibility: Requires Docker for policy validation
metadata:
  author: cerbos
  version: "1.1"
allowed-tools: Read Write Edit Bash Glob Grep Task WebFetch
---

# Cerbos Policy Generator

Generate production-ready Cerbos authorization policies from natural language requirements or specification documents.

## Prerequisites

Before starting, verify Docker is available:

```bash
docker --version
```

If Docker is not installed, inform the user and provide installation guidance:

- **macOS/Windows**: Download [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- **Linux**: Install via package manager or see [docs.docker.com/engine/install](https://docs.docker.com/engine/install/)

**Do not proceed with policy generation until Docker is available.**

## Capabilities

1. **Generate policies** from requirements, PDFs, or bullet points
2. **Answer questions** about existing policies and Cerbos concepts
3. **Modify policies** based on requests, ensuring tests pass
4. **Explain policies** in plain language

## Workflow Phases

Follow these phases in order. Do not skip ahead.

### Phase 1 — Spec Intake

Before writing any files, converge on a compact spec by asking clarifying questions in plain business language ("Who can delete a project?"), never schema jargon. Offer concrete options per question where possible — avoid open-ended prompts. Ask as many rounds and as many questions as the requirements genuinely need — do not rush to generation.

Capture every rule against the **Structured Intent** checklist. Each rule must answer all six questions before it is generatable:

| Intent | Question | Cerbos construct |
|---|---|---|
| **Subject** | Who is acting? | `principal` roles / derived roles |
| **Action** | What are they doing? | rule `actions` |
| **Resource** | On what object? | resource `kind` |
| **Condition** | Under what context? | CEL `condition` (omit for pure RBAC) |
| **Decision** | Allow or Deny? | rule `effect` (`EFFECT_ALLOW` / `EFFECT_DENY`) |
| **Purpose** | Why is this needed? | rule `name` + comment above the rule |

**Completeness gate** — if any of the six is missing for a rule, ask before generating. Never silently infer **Decision** or **Purpose**:

- **Decision** is security-critical. Cerbos is deny-by-default and deny rules take precedence over allow rules, so a missed deny is a hole. Always confirm whether a rule grants or revokes — do not assume allow.
- **Purpose** is the audit trail. Every rule needs a one-line rationale that survives into the generated YAML, so policies stay self-documenting and reviewable.

Produce a short spec artifact — one row per rule capturing all six elements:

```
Subject (role) → Action on Resource [Condition] | Effect | Purpose
e.g. manager → approve on expense [R.attr.amount < 1000] | ALLOW | Managers sign off small expenses without finance
```

List resources, principals/roles, and shared derived roles/variables alongside. Confirm the spec with the user before generating.

### Phase 2 — Write

Batch-write all files in a single pass, in this order:

1. `_schemas/` (principal + resources)
2. `derived_roles/` and `common_vars.yaml`
3. `resource_policies/` / `role_policies/`
4. `testdata/` fixtures
5. `*_test.yaml`

Every YAML file MUST begin with a `# yaml-language-server: $schema=...` header so LSP-aware editors validate the file. Policies use the `Policy.schema.json` URL, test suites use `TestSuite.schema.json`, and fixtures use the matching `TestFixture/*.schema.json`. See [POLICIES.md](references/POLICIES.md) and [TEST-SUITES.md](references/TEST-SUITES.md) for the exact URLs.

Carry the **Purpose** captured in Phase 1 into every rule: set a descriptive rule `name` and record the rationale as a comment above the rule. Rules must not ship without their "why" — the audit trail is part of the deliverable, not optional.

Do not validate between files.

### Phase 3 — Validate

```bash
docker run --rm -v "$(pwd):/policies" ghcr.io/cerbos/cerbos:latest compile /policies
```

Exit code 0 = done. Otherwise capture the error list and move to Phase 4.

### Phase 4 — Fix

Apply one targeted fix per iteration, in this priority order:

1. YAML / CEL syntax errors
2. Schema validation errors (`additionalProperties: false`)
3. Compile errors (missing imports, unresolved references)
4. Test failures

Rules:
- Fix shared files (`derived_roles/`, `common_vars.yaml`) before resource policies
- Never delete a test to make validation pass
- Give up and report if the same error persists after **3** different fix attempts
- When a condition fails in a non-obvious way, use the REPL (see [references/TESTING.md](references/TESTING.md)) rather than patching blindly

### Phase 5 — Finalize

Confirm exit code 0, then report what was created and any assumptions made during spec intake.

## Output

Complete policy bundle:
- `_schemas/` — Attribute schemas
- `derived_roles/` — Shared role definitions and exported variables
- `resource_policies/` — Policies with tests and fixtures
- `role_policies/` — Role policies (if using role-centric ABAC)

## References

- [references/REFERENCE.md](references/REFERENCE.md) — Index, quick reference, directory layout, workflow, checklists
- [references/POLICIES.md](references/POLICIES.md) — Policy types and design patterns
- [references/CEL.md](references/CEL.md) — CEL patterns, condition nesting, pitfalls, error priority and fix table
- [references/TEST-SUITES.md](references/TEST-SUITES.md) — `*_test.yaml` schema and fixture files
- [references/TESTING.md](references/TESTING.md) — `cerbosctl repl` usage and debugging recipes
