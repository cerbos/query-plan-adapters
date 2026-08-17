# ActiveRecord example application

> [!WARNING]
> The ActiveRecord adapter is a **work-in-progress prototype** — unreleased, unused in
> production, and free to change its interface without warning. This example shows how it is
> meant to be wired up; it is not a pattern to copy into a live system yet. See
> [`../README.md`](../README.md).

This is the ActiveRecord adapter's instance of the shared **demo domain**. It proves *plumbing* —
that the published gem installs, that `require "cerbos/active_record"` resolves from it, and that
the relation the adapter returns composes with the query methods a consumer actually reaches for.
It proves nothing about *semantics*; that is `../spec/adversarial_conformance_spec.rb` against
[`../../conformance/`](../../conformance/).

Read [`../../demo/README.md`](../../demo/README.md) first. Everything about the domain — the rows,
the principals, the policy, the expected ids — lives there and is shared with every other adapter's
example. There are **no per-adapter exceptions**
([ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)), and this example
carries no policies of its own.

## Running it

```bash
../../demo/scripts/run-example.sh activerecord
```

Needs Docker (for the PDP), `jq` and Ruby. The runner starts the pinned PDP over
`demo/policies/`, sets `CERBOS_HOST`, invokes `run.sh`, and diffs the JSON it prints against
`demo/cases.json`.

Do not run `app.rb` directly: it reads `CERBOS_HOST` and refuses to start without it, because a
default address would let the example pass against a PDP nobody meant to test.

## What is here

| File | Role |
| --- | --- |
| `run.sh` | Builds the gem, unpacks it into `vendor/`, installs the dependencies, runs `app.rb`. The only file the shared runner knows about. |
| `app.rb` | The program. Seeds SQLite in memory from `demo/seeds.json`, then emits the five usage shapes. |
| `Gemfile` | Third-party gems, plus the adapter resolved from the unpacked artifact. |

## The adapter comes from the packed artifact

`run.sh` runs `gem build`, then `gem unpack` into `vendor/`, and the `Gemfile` resolves
`cerbos-activerecord` from there
([ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md)). That directory holds
exactly the files the gemspec's `files` allowlist shipped, so a `lib/` file left out of the
allowlist fails here rather than for the first consumer who installs the gem. A `path: "../"`
would be easier to write and would prove nothing — it resolves the source tree, where the
allowlist has never been applied.

`run.sh` also asserts the reverse: that the built gem does **not** carry `example/`. The
TypeScript adapters get that from their `files` allowlist for free; RubyGems needs it checked
deliberately.

## No committed `Gemfile.lock`

Deliberate. The adapter is installed from an artifact rebuilt on every run, so a lockfile entry
for it would be wrong the moment it was written. Resolving the rest fresh is also what lets a new
ActiveRecord release inside the gemspec's declared range reach this job at all, which is the point
of running the example in the adapter's own workflow.
