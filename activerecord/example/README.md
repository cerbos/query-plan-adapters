# Example application for the Cerbos ActiveRecord adapter

A small HTTP application with three resource kinds — `photo`, `album` and `workspace` — that
uses the `cerbos-activerecord` adapter with a real Cerbos PDP and a real database.

Each request that needs authorization does the same four steps:

1. it makes a query plan for the principal, the resource kind and the action;
2. it changes that plan into an `ActiveRecord::Relation` with the adapter;
3. it adds the tenant boundary of the application;
4. it runs the query.

The Ruby code contains no copy of the rules. Change a file in [`policies/`](policies/), and the
rows in the responses change. The application stays the same.

> [!WARNING]
> **This is a demonstration of identity only. Do not copy this pattern.** The endpoints read
> `user`, `role`, `tenant`, `department`, `scope` and `tags` from the query string, because the
> smoke tests must change the principal from `curl`. In a true application, you must make the
> principal from your authentication layer: a token that you verified, a session, or a client
> certificate. A caller can write anything in a query string, and thus a query string is not an
> identity. Here `?role=admin` gets the rule for an administrator, and `?tenant=...` crosses
> the tenant boundary. Never read an identity or a role from a parameter, a header or a body.

## What this example shows

| Subject | Where | What the adapter makes |
| --- | --- | --- |
| A column and a boolean column | `R.attr.ownerId`, `R.attr.published` | A usual predicate |
| A path with dots through a `belongs_to` | `R.attr.ownerDepartment` | A correlated scalar subquery |
| A lambda on a many-to-many association | `R.attr.tags.exists(t, ...)` | A correlated `EXISTS` subquery with a join |
| A membership test on an association | `P.id in R.attr.collaboratorIds` | A correlated `EXISTS` subquery |
| A hierarchy | `hierarchy(R.attr.scope).descendentOf(...)` | A `LIKE` with an `ESCAPE` clause |
| An unconditional allow | The `moderate` action | `model.all`, and then the boundary of the application |
| The fail-closed guarantee | The `search-regex` action | An error, and HTTP 422 with no rows |

### The tenant boundary is outside the Cerbos filter

The `moderate` rule has no condition. Thus the planner gives `KIND_ALWAYS_ALLOWED`, and the
Cerbos filter selects every row. The application adds `where(tenant: ...)` **after** the
translation. Thus the boundary also applies to that plan. If the application put the boundary
inside the policy, an unconditional allow would cross the tenant boundary.

This is the one condition that the application owns. All the other rules are in the policies.

### The fail-closed guarantee from end to end

The `search-regex` rule uses `matches()`. That function uses RE2, and no SQL dialect gives the
behaviour of RE2. Thus the adapter raises an error and the application gives HTTP 422 with no
rows:

```json
{
  "error": "Cerbos::ActiveRecord::UnsupportedOperatorError",
  "message": "Unsupported operator: matches. Supply an operator override if the database can express it faithfully."
}
```

The adapter does not give a filter that is only approximately correct. Such a filter would give
rows that the PDP denies.

## Run the tests

Docker is the only software that you must install. The version of the PDP comes from
`conformance/CERBOS_VERSION`.

```bash
./scripts/smoke.sh
```

The script starts the PDP and the application, sends real HTTP requests, and compares the
identifiers in each response with the expected list. The same script runs in CI. If the smoke
tests fail, the script writes the logs of the application and of the PDP.

## Use the application

```bash
./scripts/run.sh
```

Then send requests to it:

```bash
curl 'http://localhost:4567/photos?action=view&user=ben&tenant=acme&tags=public'
```

Each response contains the identifiers of the rows, the kind of the plan, and the SQL:

```json
{
  "kind": "photo",
  "action": "view",
  "planKind": "KIND_CONDITIONAL",
  "ids": ["ph-banner", "ph-draft", "ph-team"],
  "sql": "SELECT \"photos\".* FROM \"photos\" WHERE ..."
}
```

### The endpoints

| Path | Resource kind |
| --- | --- |
| `/photos` | `photo` |
| `/albums` | `album` |
| `/workspaces` | `workspace` |
| `/healthz` | — |

### The parameters

| Parameter | Default | Use |
| --- | --- | --- |
| `action` | `view` | The action in the policy |
| `user` | `ana` | The identifier of the principal |
| `role` | `user` | The roles, separated by commas |
| `tenant` | `acme` | The tenant boundary of the application |
| `department` | `engineering` | `P.attr.department` |
| `scope` | `acme` | `P.attr.scope`, for the hierarchy rule |
| `tags` | (empty) | `P.attr.allowedTags`, separated by commas |

## The data

The data is small and always the same. Thus the smoke tests can compare the identifiers
exactly. [`models.rb`](models.rb) contains the schema and the rows.

| Photo | Tenant | Owner | Album | Published | Tags |
| --- | --- | --- | --- | --- | --- |
| `ph-hero` | acme | ana | al-launch | no | internal |
| `ph-banner` | acme | ana | al-launch | yes | public |
| `ph-team` | acme | ben | al-team | no | public, internal |
| `ph-draft` | acme | ben | al-team | no | — |
| `ph-globex` | globex | cara | al-secret | yes | public |

| Album | Tenant | Owner | Shared | Collaborators |
| --- | --- | --- | --- | --- |
| `al-launch` | acme | ana | no | ben |
| `al-team` | acme | ben | yes | — |
| `al-secret` | globex | cara | no | — |

| Workspace | Tenant | Owner | Scope |
| --- | --- | --- | --- |
| `w-platform` | acme | ana | `acme.engineering.platform` |
| `w-sales` | acme | ben | `acme.sales` |
| `w-globex` | globex | cara | `globex.engineering` |

| User | Tenant | Department |
| --- | --- | --- |
| `ana` | acme | engineering |
| `ben` | acme | sales |
| `cara` | globex | engineering |

## The files

| File | Content |
| --- | --- |
| [`app.rb`](app.rb) | The HTTP endpoints and the principal |
| [`authorization.rb`](authorization.rb) | The attribute map for each resource kind |
| [`models.rb`](models.rb) | The schema, the models and the data |
| [`policies/`](policies/) | The Cerbos policies |
| [`scripts/smoke.sh`](scripts/smoke.sh) | The end-to-end tests |
| [`scripts/run.sh`](scripts/run.sh) | Starts the application for manual use |
