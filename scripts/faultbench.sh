#!/usr/bin/env bash
# Build the benchmark binaries and run fault-injection scenarios (Linux/macOS).
#
#   scripts/faultbench.sh                         # quick profile, all scenarios
#   PROFILE=full REPS=5 scripts/faultbench.sh     # final numbers (DESIGN §10.3)
#   scripts/faultbench.sh S1 S3                   # selected scenarios
#
# For a real ENOSPC run of S5, mount a 64 MB tmpfs and point
# LOGGATE_BENCH_DATA_ROOT at it (needs root):
#   sudo mount -t tmpfs -o size=64m tmpfs /mnt/lgdata
#   LOGGATE_BENCH_DATA_ROOT=/mnt/lgdata scripts/faultbench.sh S5
set -euo pipefail
cd "$(dirname "$0")/.."

PROFILE="${PROFILE:-quick}"
REPS="${REPS:-0}"
mkdir -p bin
for c in loggate mocksink loadgen reconcile faultbench; do
  go build -o "bin/$c" "./cmd/$c"
done

files=()
if [ "$#" -eq 0 ]; then
  for f in bench/scenarios/S*.yaml; do files+=("$f"); done
else
  for s in "$@"; do files+=(bench/scenarios/"$s"_*.yaml); done
fi
OUT="${OUT:-bench/results/$(date +%Y%m%d-%H%M%S)-$(go env GOOS)-$PROFILE}"
exec bin/faultbench -bin bin -profile "$PROFILE" -reps "$REPS" -out "$OUT" "${files[@]}"
