# `cerbos-sqlalchemy` example application

Installs the adapter **as a built distribution** and uses it the way a consumer would, against the
shared [demo domain](../../demo/README.md).

## Run it

```bash
# from the repository root
demo/scripts/run-example.sh sqlalchemy
```

Needs `docker` (with compose), `jq`, `pdm`, and Python >= 3.10 (this directory's
`requires-python`). The runner starts the pinned Cerbos PDP; [`run.sh`](run.sh) then:

1. `pdm build`s the adapter's wheel and sdist.
2. Refuses either artifact if it contains `example/`.
3. `pdm install --check --frozen-lockfile` — installs this directory's pinned tree, failing if
   `pdm.lock` has drifted.
4. Uninstalls any previous `cerbos-sqlalchemy`, `pip install`s the wheel, and runs `pip check`.
5. Asserts `cerbos_sqlalchemy` did **not** resolve inside `../src`.
6. Runs [`main.py`](main.py), which prints one JSON document on stdout for the runner to diff
   against `demo/expected.json`.

## What it demonstrates

All [five usage shapes](../../demo/README.md#the-five-usage-shapes):

1. Plain filtered list — `get_query(plan, Document, ATTR_MAP)`.
2. `KIND_ALWAYS_ALLOWED` — `get_query` returns `select(Document)`.
3. `KIND_ALWAYS_DENIED` — `get_query` returns `select(Document).where(False)`.
4. Pagination — `.order_by()`, `.limit()` and `.offset()` on the returned `Select`.
5. The adapter's `Select` composed with an application-owned predicate:

```python
get_query(plan, Document, ATTR_MAP).where(
    Document.archived == application_filter["archived"],
    Document.region == application_filter["region"],
)
```

`main.py` never branches on the plan kind: `get_query` returns a `Select` for all three, and
`WHERE false AND <application predicate>` is still false, so a denied plan can't be resurrected by
the application's filter. The example executes that query rather than short-circuiting, to show it.

The attribute map is the one piece of configuration a consumer always writes, because Cerbos
attribute names aren't column names:

```python
ATTR_MAP = {
    "request.resource.attr.ownerId": Document.owner_id,
    "request.resource.attr.public": Document.is_public,
}
```

`archived` and `region` are application columns no policy references, which is why they appear only
in shape 5.

## What it proves that the test suites cannot

The suites under [`../tests`](../tests) import the adapter from `../src`. This example resolves it
through the published surface — the modules the distribution carries and its `Requires-Dist`
resolved against this example's pinned SQLAlchemy and Cerbos SDK
([ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md)). Each check has been
verified by breaking it:

| Break | Example | `pdm run test` |
| --- | --- | --- |
| `relations.py` dropped from the distribution's include list | fails (`ModuleNotFoundError`, step 5) | 693 passed, 1 skipped |
| `example/main.py` added to that include list | fails (step 2 refuses the artifact) | unaffected — the suites build nothing |
| `PYTHONPATH=../src`, so the source tree wins the import | fails (step 5 names the path it resolved to) | unaffected — that is how they import |

Step 4 uninstalls first because the adapter's version comes from an scm tag, so every build of a
working tree has the same version and pip would otherwise skip the install and keep the previous
run's adapter — turning a packaging break into a green run on a warm machine.

## Layout

| Path | What it is |
| --- | --- |
| `run.sh` | build → install → check → run. Prints the JSON document on stdout. |
| `main.py` | The example: one method per usage shape, and the model a consumer writes. |
| `pyproject.toml` | Exact SQLAlchemy and Cerbos SDK pins, managed by Renovate. |
| `pdm.lock` | Committed. |

`main.py` deletes and recreates a SQLite file (`demo.db`) on each run — a file rather than
`:memory:`, so a failed run leaves the seeded rows to inspect.

## Notes

- **`cerbos-sqlalchemy` is not in `pyproject.toml`.** The wheel is rebuilt every run, so a lockfile
  naming it would go stale immediately. It is installed with pip on top of the locked tree instead.
- **The lockfile is committed and the pins are exact** so that a Renovate bump of SQLAlchemy lands as
  a PR touching this directory, and the `example` job in
  [`.github/workflows/sqlalchemy_pr.yaml`](../../.github/workflows/sqlalchemy_pr.yaml) blocks the
  automerge if real usage breaks. A `>=` floor would absorb new releases silently. The job must stay
  in that workflow.
- **Scope.** This is a JSON-printing CLI, not an onboarding artifact — that is
  [`spring-data/example/`](../../spring-data/example/) (see
  [ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)). It runs SQLite under
  one SQLAlchemy major; the supported range (`sqlalchemy>=1.4`) is proved by the adapter's own
  workflow, which runs both majors.
