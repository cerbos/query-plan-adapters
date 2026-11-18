# Conformance harnesses

One directory per adapter: the suite that replays every golden in `../golden/` against the
adapter's real store, the adapter's `conformance-ledger.json`, the store setup, and fuzz mode. The
contract every harness keeps is in [../README.md](../README.md#the-harness-contract), and why the
harnesses live here rather than in the adapters is
[ADR 0016](../../docs/adr/0016-conformance-harnesses-live-outside-the-adapters.md).

A harness imports the adapter from its folder and runs on the adapter's install, so install the
adapter first. `../scripts/run-harness.sh <adapter> [store...]` starts the store, runs the harness
and tears the store down; each adapter README's "Development" section lists the commands behind it.

| Language | How the harness reaches the adapter |
|---|---|
| TypeScript | relative imports of `<adapter>/src` and `<adapter>/test`; `node_modules` is a committed link to the adapter's, so `tsc`, jest and the Convex and Prisma CLIs resolve the adapter's packages |

## Adding an adapter

1. Create `<adapter>/` here with the replay suite and a `conformance-ledger.json` naming the cases the
   adapter refuses. Its presence puts the adapter in the roster, which `validate-corpus.sh`,
   `validate-demo.sh` and `run-harness.sh` read.
2. Put the mapping the unit suite also needs in `<adapter>/test/fixtures/`, and import it from here.
3. Add the adapter's store legs to `run-harness.sh` and its workflow, with
   `conformance/harnesses/<adapter>/**` in the workflow's paths after `!conformance/harnesses/**`.

The domain stress kit's harnesses, run by hand, are in `../domains/harnesses/` and follow the same
import rule.
