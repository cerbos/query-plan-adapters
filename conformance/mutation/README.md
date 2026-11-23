# Mutation runs

The corpus proves an adapter returns the rows the PDP allows. A mutation run asks the converse: does
the corpus notice when a translator is wrong? It changes one operator in an adapter's translator,
replays that adapter's conformance harness against the mutant, and repeats for every operator it
can change. A mutant the harness still passes is a translation the corpus does not pin: either an
equivalent change, or a hole the corpus should close with a case.

```bash
go -C conformance/mutation run . -adapter ent -workers 3 -out /tmp/mut-ent      # every mutant
go -C conformance/mutation run . -adapter drizzle -files comparison,values -ops logical,negation
go -C conformance/mutation run . -adapter sqlalchemy -list                       # print, do not run
go -C conformance/mutation run . -adapter ent -out /tmp/mut-ent -report          # rewrite report.md
QPA_FUZZ_DIR=/tmp/fuzz go -C conformance/mutation run . -adapter ent \
  -survivors-of /tmp/mut-ent/results.jsonl -out /tmp/mut-ent-fuzz                # survivors against a fuzz round
```

A run is never committed and never gates a pull request. What it finds becomes a case, as a fuzz
finding does ([ADR 0014](../../docs/adr/0014-fuzzing-finds-cases-the-corpus-keeps-them.md)).

## What it mutates

One operator per mutant, in the files the adapter's entry in `main.go` names (tests and the corpus
loader excluded):

| Operator | Mutation |
|---|---|
| `logical` | `&&` ↔ `\|\|`, `and` ↔ `or` |
| `equality` | `==` ↔ `!=`, `===` ↔ `!==`, `is` ↔ `is not` |
| `boundary` | `<` ↔ `<=`, `>` ↔ `>=` |
| `negation` | drop a unary `!` or `not` |
| `boolean` | `true` ↔ `false` |
| `guard` | `if c` becomes `if false && (c)`: the branch never runs |

`guard` is the one that matters most for the invariant: dropping the branch that throws the refusal
type turns a refused shape into an emitted filter, and only a case of that shape notices. Go error
propagation (`err != nil`) is not mutated.

The mutator is lexical: it skips comments, strings, template and regex literals, and mutates `<` and
`>` only between spaces, so generic type arguments are left alone. A mutant that does not build is
reported as `nonviable` and left out of the score.

## How it runs

Each worker holds a copy of the adapter beside symlinks to the rest of the repository (and to the
adapter's `node_modules` or `.venv`), so mutants run in parallel without touching the checkout. The
unmutated harness runs first and must pass; a mutant gets four times its time plus 30 seconds.

Each adapter replays one fast store leg, the leg that needs no container:

| Adapter | Harness command |
|---|---|
| ent | `go test -run TestAdversarialConformance/sqlite` |
| pgx | `go test -run TestAdversarialConformance` (its own `render.go` and `adapter.go`; the vendored translator is ent's) |
| sqlalchemy | `pytest tests/test_adversarial_conformance.py -k sqlite-declared-0.58.0` |
| drizzle | `jest --config jest.adversarial.config.js` (SQLite) |

A survivor on SQLite can still be killed by another store's leg; a dialect branch is the usual
reason. Before writing a case, replay the survivor against the adapter's other legs.

`<out>/results.jsonl` holds one line per mutant, and a rerun with the same `-out` resumes where it
stopped. `<out>/report.md` lists the survivors.

## Triage

For each survivor, decide which it is:

- **Equivalent**: the mutant translates every plan the planner can emit the same way, for example a
  rewrite that is exact either way, or a guard whose condition the planner never satisfies. Nothing
  to do.
- **Unreached**: no case drives the translator into that line. Write the shape that does.
- **Weak oracle**: a case reaches the line, but no seed tells the two translations apart. Add a seed
  or a case whose allowed set differs between them.

A new case follows ["Changing how a condition is translated"](../README.md#changing-how-a-condition-is-translated):
add it under `cases/`, regenerate, and run every adapter's harness.
