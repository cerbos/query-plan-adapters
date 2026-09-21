#!/bin/sh
set -eu

# Check and Plan must use the same explicit engine mode throughout a corpus run.
strict_evaluation=${ADAPTER_TEST_STRICT_EVALUATION-false}
case "$strict_evaluation" in
  true|false) ;;
  *)
    echo "Invalid ADAPTER_TEST_STRICT_EVALUATION '$strict_evaluation': expected true or false" >&2
    exit 2
    ;;
esac

exec cerbos run "--set=engine.strictEvaluation=$strict_evaluation" "$@"
