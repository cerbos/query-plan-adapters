# Cerbos Policy Types

Every Cerbos policy is YAML with an `apiVersion: api.cerbos.dev/v1` header and exactly one top-level policy object.

## Required YAML Language-Server Header

Every policy file MUST begin with the Cerbos policy schema comment so LSP-aware editors provide validation and autocomplete:

```yaml
# yaml-language-server: $schema=https://api.cerbos.dev/latest/cerbos/policy/v1/Policy.schema.json
```

This applies to ALL policy files: resource policies, derived roles, exported variables, role policies, and principal policies. Test suites and fixtures use different schemas — see [TEST-SUITES.md](TEST-SUITES.md).

## Resource Policy (most common)

```yaml
# yaml-language-server: $schema=https://api.cerbos.dev/latest/cerbos/policy/v1/Policy.schema.json
apiVersion: api.cerbos.dev/v1
resourcePolicy:
  version: "default"
  resource: "document"
  schemas:
    principalSchema:
      ref: "cerbos:///principal.json"
    resourceSchema:
      ref: "cerbos:///resources/document.json"
  importDerivedRoles:
    - document_roles
  rules:
    - actions: ["view"]
      effect: EFFECT_ALLOW
      roles: ["user"]
      condition:
        match:
          expr: R.attr.owner == P.id
```

Always include the `schemas` field:

- `principalSchema.ref`: `cerbos:///principal.json`
- `resourceSchema.ref`: `cerbos:///resources/{resource_name}.json` (must match the resource name)

## Derived Roles

```yaml
# yaml-language-server: $schema=https://api.cerbos.dev/latest/cerbos/policy/v1/Policy.schema.json
apiVersion: api.cerbos.dev/v1
derivedRoles:
  name: "document_roles"
  definitions:
    - name: owner
      parentRoles: ["user"]
      condition:
        match:
          expr: R.attr.owner == P.id
```

Derived roles are referenced by resource policies via `importDerivedRoles`.

## Exported Variables

```yaml
# yaml-language-server: $schema=https://api.cerbos.dev/latest/cerbos/policy/v1/Policy.schema.json
apiVersion: api.cerbos.dev/v1
exportVariables:
  name: "common_vars"
  definitions:
    is_owner: R.attr.owner == P.id
```

Only `import` exported variables in files that actually use `V.*` — unused imports cause compile noise. See also the extraction rule in [CEL.md](CEL.md#exported-variables-extraction-rule).

## Role Policy (IdP role-centric ABAC)

Role policies define permissions from the perspective of an IdP role. Unlike resource/principal policies, they use an allowlist model — any resource-action pair not explicitly listed is denied.

```yaml
# yaml-language-server: $schema=https://api.cerbos.dev/latest/cerbos/policy/v1/Policy.schema.json
apiVersion: api.cerbos.dev/v1
rolePolicy:
  role: "acme_admin"
  scope: "acme"           # optional: principal scope
  parentRoles:            # optional: inherit and narrow permissions
    - "admin"
  rules:
    - resource: "document"
      allowActions:
        - "view"
        - "edit"
        - "delete"
    - resource: "report"
      allowActions:
        - "view"
        - "view:*"        # wildcard
      condition:
        match:
          expr: R.attr.department == P.attr.department
```

Key characteristics:

- **Allowlist model**: no `EFFECT_ALLOW`/`EFFECT_DENY` — `allowActions` is the exhaustive permitted list
- **Implicit deny**: any action not in `allowActions` is denied
- **Parent inheritance**: child roles can only NARROW parent permissions (strict subset)
- **Wildcards**: both `resource` and `allowActions` support wildcards (`view:*`)
- **Conditions**: optional CEL expressions per rule entry
- **`allowActions` must be non-empty**: `allowActions: []` is a validation error

## Scoped Role Policies (`parentRoles` + `scope`)

Scoped role policies let tenants create custom roles that narrow base role permissions.

**Inheritance behavior:**

- Resources LISTED in the child policy are narrowed to the child's `allowActions` (must be a strict subset of the parent)
- Resources NOT LISTED in the child policy are DENIED entirely — the allowlist is exhaustive
- To grant any access to a resource, it must be explicitly listed in the child policy's rules

**Derived roles do NOT need custom role names:**

- If base derived roles list `parentRoles: ["admin", "operator", "developer", "viewer"]`, custom roles with `parentRoles: ["operator"]` automatically resolve through the parent chain
- Never add custom role names to derived role `parentRoles` lists

**Scope goes on the RESOURCE fixture, not the principal:**

- Role policy declares `scope: "acme"`
- Test resource fixtures must have `scope: "acme"` for the scoped role policy to evaluate
- Test principal fixtures should NOT have a `scope` field — just the custom role name in `roles`

```yaml
# role_policies/acme/release_manager.yaml
apiVersion: api.cerbos.dev/v1
rolePolicy:
  role: "release_manager"
  scope: "acme"
  parentRoles:
    - "operator"
  rules:
    - resource: "flow"
      allowActions:
        - "read"
        - "deploy"
    - resource: "secret"
      allowActions:
        - "read"
    # connector NOT listed → denied (allowlist is exhaustive)
```

```yaml
# testdata/principals.yaml — no scope on principal
acme_release_manager:
  id: "user_acme_rm"
  roles:
    - "release_manager"
  attr:
    org_id: "org1"
```

```yaml
# testdata/resources.yaml — scope on resource
acme_staging_flow:
  kind: "flow"
  id: "flow1"
  scope: "acme"
  attr:
    org_id: "org1"
    project_id: "proj1"
    environment: "staging"
```

**Conditions on scoped role rules:**

Individual rules within a scoped role policy can have CEL conditions. Use separate rule entries for the same resource to apply conditions to specific actions only.

```yaml
rules:
  - resource: "flow"
    allowActions:
      - "read"
  - resource: "flow"
    allowActions:
      - "create"
      - "update"
    condition:
      match:
        expr: R.attr.environment == "staging"
```

**File organization:** group scoped role policies into subfolders per tenant: `role_policies/acme/`, `role_policies/globex/`.

## Policy Design Patterns

Choose the pattern that best fits the requirements.

### Action-Led (recommended default)

Focus on actions, list which roles can perform each. Best when:

- Roles have hierarchical permissions
- You need visibility into "high-risk" actions
- Many roles, fewer distinct actions

### Role-Led

Center on roles, specify their allowed actions. Best when:

- Roles are distinct with minimal overlap
- Fewer roles, many actions
- Clear role separation

### Attribute-Led (ABAC)

Use dynamic attributes instead of static roles. Best for:

- Multi-tenant systems
- Context-aware access (time, location, status)
- Flexible rules without policy changes

### Role Policy (IdP role-centric)

Use role policies when permissions should be defined from the IdP role's perspective. Best for:

- Integrating with IdP-managed roles
- Simple allowlist semantics (list what's allowed, everything else denied)
- Hierarchical roles with inheritance and narrowing
- When you want to avoid explicit DENY rules
