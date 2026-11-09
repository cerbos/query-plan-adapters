# MongoDB Ruby example application

> [!WARNING]
> The MongoDB Ruby adapter is a **work-in-progress prototype**. This example shows how it is meant
> to be wired up; it is not a pattern to copy into a live system yet. See [`../README.md`](../README.md).

This is the adapter's instance of the shared **demo domain**. It proves *plumbing* — that the
published gem installs, that `require "cerbos/mongodb"` resolves from it, and that the filter it
returns composes with the driver calls a consumer reaches for (`find`, `$and` with the
application's own predicate, `sort`/`skip`/`limit`) against a **real MongoDB server**. It proves
nothing about *semantics*; that is `../spec/conformance_spec.rb` against
[`../../conformance/`](../../conformance/).

Read [`../../demo/README.md`](../../demo/README.md) first. The rows, the principals, the policy and
the expected ids live there and are shared with every other adapter's example, with **no
per-adapter exceptions** ([ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)).

## Running it

```bash
../../demo/scripts/run-example.sh mongodb-ruby
```

Needs Docker, `jq` and Ruby. The shared runner starts the pinned PDP over `demo/policies/` and sets
`CERBOS_HOST`; `run.sh` starts MongoDB from `../MONGO_IMAGE` — the same pinned server the adapter's
baseline CI leg tests against — on a port Docker picks, and removes it afterwards.

| File | Role |
| --- | --- |
| `run.sh` | Starts MongoDB, builds the gem, unpacks it into `vendor/`, installs the dependencies, runs `app.rb`. |
| `app.rb` | Seeds the `documents` collection from `demo/seeds.json`, then emits the five usage shapes — each computed through the driver and through a Mongoid criteria (`Cerbos::MongoDB::Mongoid.criteria`), which must agree. |
| `Gemfile` | The driver, Mongoid and the Cerbos SDK, plus the adapter resolved from the unpacked artifact. |

`run.sh` runs `gem build` and `gem unpack`, and the `Gemfile` resolves `cerbos-mongodb` from
`vendor/` ([ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md)), so a `lib/`
file missing from the gemspec's `files` allowlist fails here. It also asserts the gem ships none of
`example/`, `spec/`, `scripts/` or `conformance-ledger.json`. There is deliberately no
`Gemfile.lock`: the adapter is rebuilt on every run, and resolving the rest fresh is what lets a new driver release reach this job.
