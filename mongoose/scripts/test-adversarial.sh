#!/usr/bin/env bash

# Run the adversarial suite against the pinned MongoDB image named by the profile. The workflow
# performs these steps inline; this script gives adapterctl the same setup and cleanup as one
# native command without making the low-level npm test own an external server.

set -euo pipefail

cd "$(dirname "$0")/.."

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <MONGO_IMAGE|MONGO_NEXT_IMAGE>" >&2
  exit 2
fi

case "$1" in
  MONGO_IMAGE | MONGO_NEXT_IMAGE) image_file="$1" ;;
  *)
    echo "Unknown MongoDB image file '$1' (expected MONGO_IMAGE or MONGO_NEXT_IMAGE)" >&2
    exit 2
    ;;
esac

image="$(tr -d '[:space:]' <"${image_file}")"
if [[ -z "${image}" ]]; then
  echo "MongoDB image file '${image_file}' is empty" >&2
  exit 2
fi

container_name="mongoose-adversarial-$$"
container_started=0

cleanup() {
  if [[ "${container_started}" -eq 1 ]]; then
    docker rm -f "${container_name}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

docker run -d --name "${container_name}" -p 27017:27017 "${image}" >/dev/null
container_started=1

ready=0
for _ in {1..30}; do
  if docker exec "${container_name}" mongosh --quiet --eval 'db.runCommand({ ping: 1 }).ok' \
    >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

if [[ "${ready}" -ne 1 ]]; then
  echo "MongoDB failed to start from ${image_file}" >&2
  docker logs "${container_name}" >&2 || true
  exit 1
fi

npm run test:adversarial
