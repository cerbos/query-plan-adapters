# `cerbos-polars` example application

Installs the adapter **as a built distribution** and uses it the way a consumer would, against the
shared [demo domain](../../demo/README.md).

## Run it

```bash
# from the repository root
demo/scripts/run-example.sh polars
```

Needs `docker` (with compose), `jq`, `pdm`, and Python >= 3.11 (this directory's
`requires-python`). The runner starts the pinned Cerbos PDP; [`run.sh`](run.sh) then:

1. `pdm build`s the adapter's wheel and sdist.
2. Refuses either artifact if it contains `example/`.
3. `pdm install --check --frozen-lockfile`: installs this directory's pinned tree, failing if
   `pdm.lock` has drifted.
4. Uninstalls any previous `cerbos-polars`, `pip install`s the wheel, and runs `pip check`.
5. Asserts `cerbos_polars` did **not** resolve inside `../src`.
6. Runs [`main.py`](main.py), which prints one JSON document on stdout for the runner to diff
   against `demo/expected.json`.

## What it demonstrates

All [five usage shapes](../../demo/README.md#the-five-usage-shapes), on a `LazyFrame`:

1. Plain filtered list: `frame.filter(get_filter(plan, frame, ATTR_MAP))`.
2. `KIND_ALWAYS_ALLOWED`: `get_filter` returns `pl.lit(True)`.
3. `KIND_ALWAYS_DENIED`: `get_filter` returns `pl.lit(False)`.
4. Pagination: `.sort("id").slice(offset, size)` on the filtered frame.
5. The adapter's expression composed with an application-owned predicate:

```python
frame.filter(
    get_filter(plan, frame, ATTR_MAP)
    & (pl.col("archived") == application_filter["archived"])
    & (pl.col("region") == application_filter["region"])
)
```

`main.py` never branches on the plan kind: `pl.lit(False) & <application predicate>` is still
false, so a denied plan can't be resurrected by the application's filter.

The attribute map names the columns, because Cerbos attribute names aren't column names:

```python
ATTR_MAP = {
    "request.resource.attr.ownerId": "owner_id",
    "request.resource.attr.public": "is_public",
}
```

## Notes

- **`cerbos-polars` is not in `pyproject.toml`.** The wheel is rebuilt every run, so a lockfile
  naming it would go stale immediately. It is installed with pip on top of the locked tree instead.
- **The lockfile is committed and the pins are exact** so that a Renovate bump of Polars lands as a
  PR touching this directory, and the `example` job in
  [`.github/workflows/polars_pr.yaml`](../../.github/workflows/polars_pr.yaml) blocks the automerge
  if real usage breaks.
