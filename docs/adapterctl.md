# Maintaining adapter certification

`adapterctl` is the repository-only control plane for the conformance corpus, adapter
certification, and consumer smoke tests. It discovers adapter-local `adapterctl.json` manifests;
published adapter packages do not import it.

The versioned action catalog, consumer cases, canonical resources, and adapter manifests are the
control plane's only inputs. Native harnesses read them directly; no compatibility ledger or
duplicated count and liveness lists sit between the declared contract and executed tests.
Discovery mode relaxes only the requirement that every action is already assessed; it never makes
CI certification non-strict.

Requires Node.js 22.18 or newer. Install the repository-only CLI dependencies after a fresh
checkout:

```bash
npm ci --prefix tools/adapterctl
```

## Commands

```bash
./adapterctl validate
./adapterctl validate --discovery
./adapterctl list
./adapterctl explain adapter prisma
./adapterctl explain action vf-le
./adapterctl report --format markdown
./adapterctl run --adapter drizzle --profile test
./adapterctl run --profile conformance --action vf-le --dry-run
./adapterctl docs --check
```

Use `run` for orchestration, not for replacing native test implementations. Each manifest points
to the adapter's own command and declares the semantic environments in which that command is
certified. The adapter still owns datastore setup, translation, execution, packaging, and cleanup.

## Add a conformance action

1. Add the policy rule to `conformance/policies/adversarial.yaml`, plus discriminating seed and
   derived data when needed.
2. Add the action once to `conformance/catalog.json` with its global oracle cardinality
   expectation, then regenerate the wire fixture and canonical check resources.
3. Run `./adapterctl validate --discovery`. A missing adapter outcome is reported as `unassessed`;
   it is never silently skipped.
4. Run `./adapterctl run --profile conformance --action <action>`. Triage every adapter result as
   `matched`, `rejected`, or `upstream-blocked`; keep a rejection's reason and observed error
   substring together in that adapter's manifest.
5. Regenerate affected golden assets using the commands reported by `adapterctl`, review their
   diffs, and run `./adapterctl validate`. Strict validation must reach 100% action accounting.
6. Run `./adapterctl docs --write` and commit the generated certification report.

The cardinality expectation is global because the PDP oracle is global. Whether an adapter
matches or rejects that action is local because it changes only that adapter's confidence profile.

## Add an adapter

1. Scaffold the adapter directory and its native translator, conformance harness, and example
   application.
2. Add `adapterctl.json` beside the package manifest. Declare package identity, the owning
   workflow, native commands, every semantics-bearing environment, golden generation, consumer
   coverage, and direct action outcomes.
3. Use `artifact-install` consumer coverage when the example installs a built distribution. Ent
   and pgx use `usage-only` because Go modules have no repository packaging step.
4. Add the adapter-owned workflow with `<adapter>/**`, `conformance/**`, and `demo/**` triggers.
   Keep the example job in that workflow so dependency-update pull requests retain their gate.
5. Run `./adapterctl validate --adapter <adapter>`, then the manifest's test, conformance, and
   consumer commands. Finish with strict repository-wide validation and generated docs.

An adapter is discovered from its manifest. There is no second root roster to update.
Each adapter-owned workflow invokes the scoped certification action, so an adapter-local manifest
change runs only that adapter's workflow. Catalog, schema, resource, case, tooling, and generated
documentation changes run the repository control-plane workflow.

## Add a shared consumer case

1. Add one entry to `demo/cases.json` with the operation, principal, action, pagination input, plan
   kind, and expected ids.
2. Implement the operation through each example's native ORM interface. Filtering, pagination,
   composition, and package installation remain adapter-local.
3. Run `demo/scripts/validate-demo.sh`; it validates the catalog's structure and non-degeneracy.
4. Run `./adapterctl run --profile consumer` and `./adapterctl validate`.
5. Regenerate certification documentation.

The consumer catalog describes shared inputs and observable output only. It must not grow
language, ORM, or datastore branches.
