# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

import sys


def pytest_sessionfinish(session, exitstatus):
    """Writes the harness's fuzz verdicts, when it replayed a fuzz run."""
    harness = sys.modules.get("test_conformance")
    if harness is not None and harness.FUZZ_RESULTS:
        from corpus import write_fuzz_results

        write_fuzz_results("duckdb", harness.FUZZ_RESULTS)


def pytest_terminal_summary(terminalreporter):
    """Per-tier outcomes of the conformance harness, when it ran."""
    harness = sys.modules.get("test_conformance")
    if harness is None or not harness.RESULTS:
        return
    terminalreporter.section("conformance")
    for run, tally in sorted(harness.RESULTS.items()):
        tiers = sorted({tier for tier, _ in tally})
        summary = ", ".join(
            f"{tier} {tally[(tier, 'pass')]}/{sum(n for (t, _), n in tally.items() if t == tier)}"
            for tier in tiers
        )
        terminalreporter.write_line(f"{run}: {summary} passed")
