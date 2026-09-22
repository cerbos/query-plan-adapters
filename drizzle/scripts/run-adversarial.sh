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

# The PDP listens on Unix sockets in a directory this run creates, never on a TCP port
# (cerbos/query-plan-adapters#476). `cerbos run` does not fail when its port is already bound: its
# startup health probe is answered by whichever PDP holds the port, and the suite then plans
# against that one — another run's, possibly on another corpus revision or evaluation mode — and
# can pass. A fresh directory cannot be held by anything else. `cerbos run` exports the socket as
# CERBOS_GRPC, which the harness requires rather than defaulting.
socket_dir=$(mktemp -d "${TMPDIR:-/tmp}/cerbos-pdp.XXXXXX")
trap 'rm -rf "$socket_dir"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cerbos run "--set=engine.strictEvaluation=$strict_evaluation" \
  "--set=server.grpcListenAddr=unix:$socket_dir/grpc.sock" \
  "--set=server.httpListenAddr=unix:$socket_dir/http.sock" \
  "$@"
