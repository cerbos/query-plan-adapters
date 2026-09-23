# ActiveRecord example application

> [!WARNING]
> The ActiveRecord adapter is a **work-in-progress prototype**: unreleased, not used in
> production, and free to change its interface without warning. This example shows how it is
> meant to be wired up; do not copy it into a live system yet. See [`../README.md`](../README.md).

## Running it

From the repository root:

```bash
demo/scripts/run-example.sh activerecord
```

Needs Docker with compose (for the PDP), `jq`, and Ruby 3.3+ with `gem` and `bundle`. The runner
starts the pinned PDP over `demo/policies/`, sets `CERBOS_HOST`, runs `run.sh`, and diffs the JSON
it prints against `demo/expected.json`.

Do not run `app.rb` directly: it refuses to start without `CERBOS_HOST`, so it can never pass
against a PDP nobody meant to test.

## What it demonstrates

This is the ActiveRecord instance of the shared [demo domain](../../demo/README.md). It proves
**plumbing** — the published gem installs, `require "cerbos/active_record"` resolves from it, and
the returned relation composes with real query methods. Semantics are proved by
`../spec/adversarial_conformance_spec.rb` against [`../../conformance/`](../../conformance/).

`app.rb` seeds SQLite in memory from `demo/seeds.json`, maps `ownerId` → `owner_id` and
`public` → `is_public`, then emits the five usage shapes:

1. **filtered** — the adapter's relation is the whole query.
2. **alwaysAllowed** — an unconditional allow returns every row through the same code path.
3. **alwaysDenied** — an action with no rule returns nothing.
4. **paginated** — `.order(:id).offset(...).limit(...)` over the relation, page by page.
5. **composed** — the adapter's relation combined with the application's own `where` from
   `demo/seeds.json`; the query runs even for a denial, to show the application filter cannot
   bring a denied row back.

Everything about the domain (rows, principals, policy, expected ids) lives in `demo/` and is shared
with every example. There are no per-adapter exceptions
([ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)), and this example
has no policies of its own.

| File | Role |
| --- | --- |
| `run.sh` | Builds the gem, unpacks it into `vendor/`, runs `bundle install`, then `app.rb`. The only file the shared runner calls. |
| `app.rb` | The program. |
| `Gemfile` | Third-party gems, plus the adapter resolved from the unpacked artifact. |

## Packaging checks

- **The adapter comes from the packed artifact**
  ([ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md)). `run.sh` runs
  `gem build`, then `gem unpack` into `vendor/`, and the `Gemfile` resolves `cerbos-activerecord`
  from there. That directory holds exactly what the gemspec's `files` allowlist shipped, so a
  missing `lib/` file fails here instead of for the first consumer. A `path: "../"` would bypass
  the allowlist and prove nothing.
- **The gem must not carry `example/`.** `run.sh` inspects the built gem and fails if it does.
- **No committed `Gemfile.lock`.** The adapter gem is rebuilt on every run, so a lock entry for it
  would be stale immediately. Resolving fresh also lets a new ActiveRecord release inside the
  gemspec's range reach this job.
