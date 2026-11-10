# Fuzzing finds cases; the corpus keeps them

Accepted. Builds on [ADR 0010](0010-conformance-replays-recorded-pdp-decisions.md).

## Context

The corpus proves every adapter against the shapes someone wrote down: 477 cases, each about one
construct. Customer policies combine constructs, and the bugs that reached adapters were
combinations nobody had written yet: RE2 repetition counts (#597), a NULL list in a native array
column (#613), int arithmetic beside a double (#567). Each was found by a person, then added as a
case. Nothing searched for the next one.

## Decision

**A fuzzer generates conditions, the pinned PDPs answer them, and every harness replays the answers.
What it finds is promoted by hand into the corpus.**

- The generator gains `fuzz`, `fuzz-report` and `fuzz-shrink` subcommands. `fuzz` composes cases from
  hand-written expressions and a typed grammar, seeded, and records them with the same code that
  records the corpus, against the same two release images. Nothing it writes lands under
  `conformance/`.
- Harnesses keep one replay path. `QPA_FUZZ_DIR` points it at a fuzz run, and a verdict replaces
  the ledger: a refusal is always acceptable, because no ledger can list shapes no one has seen.
- `fuzz.yaml` is started by hand. It never runs on pull requests or on a schedule.
- A failure becomes a case only through a person, who names it and writes its intent and trap.

## Consequences

**PR CI stays deterministic.** Random cases never gate a merge; the corpus they grow does.

**Still no PDP in adapter CI.** Only the fuzz workflow's record job starts one, as `conformance.yaml`
does.

**The grammar is limited to what harnesses map.** An attribute, macro or lambda variable no
hand-written case reads would fail on a harness mapping gap, not on the adapter, so the grammar
leaves it out, and the generator stops if a case it relies on is removed. Widening the grammar
starts with a hand-written case.

**Agreement is read as the planner.** When every translating leg returns the same wrong rows, the
report files the case as a suspected planner divergence and does not fail on it. The first runs
found these where a DENY condition errors for a row: `check()` drops the rule, while the plan
negates the error and denies the row (the #530 family).

**What it does not cover.** The dataset has 44 seeds, so two filters that differ only on rows no
seed holds look the same. Conditions over `now()` are left out, because their recorded value is a
placeholder.
