# `cerbos-sqlalchemy` example application

A runnable program that installs the adapter **as a published distribution** and uses it the way a
consumer would, against the shared [demo domain](../../demo/README.md).

```bash
# from the repository root
demo/scripts/run-example.sh sqlalchemy
```

Needs `docker` (with compose), `jq`, `pdm` and a Python matching this directory's
`requires-python`. The runner starts the pinned Cerbos PDP; this directory's `run.sh` builds the
adapter, installs the wheel, and runs the program.

## What it proves

Not what the adapter translates — [`../tests/test_adversarial_conformance.py`](../tests/test_adversarial_conformance.py)
proves that against a hostile corpus with a live PDP as the oracle, and
[`../tests/test_translator.py`](../tests/test_translator.py) pins the SQL it emits. This proves the
two things every suite under [`../tests`](../tests) structurally cannot:

**Packaging.** `run.sh` builds the sdist and wheel `pdm publish` would upload and installs *that*
into this directory's own environment, so the import in [`main.py`](main.py) resolves through the
published surface: which modules the distribution actually carries, and the `Requires-Dist`
metadata resolved against this example's pinned SQLAlchemy and Cerbos SDK. Every one of those
suites imports the adapter from `../src` and touches none of it. See
[ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md).

Both halves are load-bearing and both have been checked by breaking them:

| Break                                                       | Example                                     | `pdm run test`                        |
| ----------------------------------------------------------- | ------------------------------------------- | ------------------------------------- |
| `relations.py` dropped from the distribution's include list | fails (`ModuleNotFoundError`, step 5)       | 693 passed, 1 skipped                 |
| `example/main.py` added to that include list                | fails (step 2 refuses the artifact)         | unaffected — the suites build nothing |
| `PYTHONPATH=../src`, i.e. the source tree wins the import   | fails (step 5 names the path it resolved to) | unaffected — that is how they import  |

The first row is the Python translation of a broken `files` allowlist, and it is the reason step 4
of `run.sh` uninstalls before it installs. The adapter's version comes from an scm tag, so every
build of a working tree produces the *same* version — and pip treats an installed distribution of
that version as satisfying a local wheel path, skipping the install and leaving the previous run's
adapter in place. CI is always cold and would never see it; the tree where someone checks a
packaging break by hand is not, and there it turns the break into a green run.

Step 5 is the other half. Unlike a compiled language, Python will import a source tree that merely
happens to be on `sys.path`, and `../src` is one `PYTHONPATH` away — so before anything is planned,
the example asserts that `cerbos_sqlalchemy` did **not** resolve inside `../src`. Stated as "not
the source tree" rather than "inside `site-packages`", because the first is the hazard and the
second is an install layout.

**Usage shape.** A harness runs one flat filtered query. This runs all
[five shapes](../../demo/README.md#the-five-usage-shapes), including pagination and — the one that
earns the exercise — the adapter's `Select` composed with the application's own predicate, across
all three plan kinds.

## Layout

| Path             | What it is                                                                        |
| ---------------- | --------------------------------------------------------------------------------- |
| `run.sh`         | build → install → check → run. Prints the JSON document on stdout.                 |
| `main.py`        | The example itself: one method per usage shape, and the model a consumer writes.   |
| `pyproject.toml` | The ORM and SDK pins Renovate manages — exact, so a release always lands as a PR.  |
| `pdm.lock`       | Committed. See below.                                                              |

The SQLite file is scratch state `main.py` deletes and recreates on every run — a dedicated file
rather than `:memory:`, so a failing run leaves the seeded rows behind to inspect.

## Two things that look odd and are not

**`cerbos-sqlalchemy` is not in `pyproject.toml`.** The wheel is rebuilt on every run, so a
committed lockfile naming it would be wrong the moment the adapter changed. `run.sh` runs
`pdm install --check --frozen-lockfile` — the `npm ci` of this toolchain, which fails rather than
re-resolving if the lockfile has drifted — and then installs the wheel on top with pip, which
leaves both manifests exactly as committed while still resolving the adapter's `Requires-Dist`
against the versions this example pins.

**The lockfile is committed anyway,** because it is what makes the CI job load-bearing.
`renovate.json` automerges every non-major bump, so a SQLAlchemy bump arrives as one PR touching
both `sqlalchemy/pyproject.toml` and this directory — and the `example` job on that PR is what
blocks the automerge when the new version breaks real usage. That only works while the job stays in
[`.github/workflows/sqlalchemy_pr.yaml`](../../.github/workflows/sqlalchemy_pr.yaml); there is a
comment on the job saying so.

It is also why the two dependencies here are pinned **exactly** rather than with the `>=` floor a
Python library declares. Renovate opens a PR when a release falls outside the declared constraint,
so `sqlalchemy>=2.0` would absorb every future 2.x silently: nothing would touch this directory,
and the job above would have nothing to gate. The supported *range* is
[`../pyproject.toml`](../pyproject.toml)'s business, and the adapter's own workflow runs both
majors against it.

## The attribute map

Cerbos attribute names are not column names, so a consumer always writes one of these:

```python
ATTR_MAP = {
    "request.resource.attr.ownerId": Document.owner_id,
    "request.resource.attr.public": Document.is_public,
}
```

Without it the adapter has nothing to resolve `request.resource.attr.ownerId` to and raises — which
is itself worth seeing in an example. The model names the columns `owner_id` and `is_public`, which
is ordinary Python naming and makes the point that a Cerbos attribute name is neither the column
name nor the attribute name on the model.

`archived` and `region` are deliberately absent: they are the application's columns, never
referenced by policy, and composing them with the adapter's query is shape 5.

## `get_query` returns a `Select` for every plan kind

Which is what makes shape 5 one line here. `get_query` returns `select(table)` for an
unconditional allow, `select(table).where(False)` for an unconditional denial, and the translated
tree for a conditional plan — so the application composes its own predicate the same way whichever
came back:

```python
get_query(plan, Document, ATTR_MAP).where(
    Document.archived == application_filter["archived"],
    Document.region == application_filter["region"],
)
```

`WHERE false AND <application predicate>` is still false, so a denied plan cannot be resurrected by
the application's own filter — and this example executes that query rather than short-circuiting on
the kind, because executing it is what demonstrates the property.

## Scope

This example is a JSON-printing CLI, not an onboarding artifact — that is
[`spring-data/example/`](../../spring-data/example/), and the floor/ceiling rule in
[ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md) is why both exist.

It runs SQLite under one SQLAlchemy major. The adapter declares `sqlalchemy>=1.4` and its own
workflow runs both majors, which is where that range is proved; this proves that the distribution
installs and composes, and a second major here would re-run the same plumbing.
