#!/usr/bin/env bash
set -euo pipefail
PROVIDER_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-verify}"
case "$MODE" in
  verify|publish) ;;
  *) echo 'Usage: bash drift/run.sh [verify|publish]' >&2; exit 2 ;;
esac

if [[ "$MODE" == publish ]]; then
  : "${PACT_BROKER_BASE_URL:?Set the PactFlow workspace URL}"
  : "${PACT_BROKER_TOKEN:?Set a read/write PactFlow token}"
  : "${GIT_COMMIT:?Set the exact tested provider version (unique for uncommitted local changes)}"
  : "${GIT_BRANCH:?Set the tested branch}"
  command -v pactflow >/dev/null || { echo 'Install the Pact CLI (pactflow command).' >&2; exit 2; }
fi

mkdir -p "$PROVIDER_ROOT/build/drift"
RUN_DIR="$(mktemp -d "$PROVIDER_ROOT/build/drift/run.XXXXXX")"
# Snapshot the tested contract so publishing cannot accidentally use an edited one.
python3 - "$PROVIDER_ROOT" "$RUN_DIR" <<'PY'
import json, os, shutil, sys, uuid
from pathlib import Path
root, run = map(Path, sys.argv[1:])
document = json.loads((root / 'asyncapi.json').read_text())
document['servers']['local']['host'] = os.environ.get('DRIFT_KAFKA_BROKERS', 'localhost:9092')
(run / 'asyncapi.json').write_text(json.dumps(document, indent=2) + '\n')
(run / 'drift.yaml').write_text((root / 'drift.yaml').read_text().replace('drift-product-created', 'drift-' + str(uuid.uuid4())))
shutil.copy2(root / 'drift/trigger.py', run / 'trigger.py')
PY
echo "Drift artifacts: $RUN_DIR"
cd "$RUN_DIR"
DRIFT_EXIT_CODE=0
# Pin CLI and bundled plugins. DRIFT_BIN permits a preinstalled/offline binary.
if [[ -n "${DRIFT_BIN:-}" ]]; then
  "$DRIFT_BIN" verify -f drift.yaml --output-dir "$RUN_DIR" --generate-result || DRIFT_EXIT_CODE=$?
else
  npx --yes @pactflow/drift@2608.3.0 verify -f drift.yaml --output-dir "$RUN_DIR" --generate-result || DRIFT_EXIT_CODE=$?
fi
printf '%s\n' "$DRIFT_EXIT_CODE" > "$RUN_DIR/exit-code.txt"

if [[ "$MODE" == publish ]]; then
  shopt -s nullglob
  RESULTS=("$RUN_DIR"/results/verification.*.result)
  if [[ ${#RESULTS[@]} -ne 1 ]]; then
    echo 'Expected exactly one result bundle from this run; nothing published.' >&2
    exit 1
  fi
  pactflow publish-provider-contract "$RUN_DIR/asyncapi.json" \
    --provider pactflow-example-provider-java-kafka \
    --provider-app-version "$GIT_COMMIT" \
    --branch "$GIT_BRANCH" \
    --content-type application/json \
    --specification asyncapi \
    --verification-exit-code "$DRIFT_EXIT_CODE" \
    --verification-results "${RESULTS[0]}" \
    --verification-results-content-type application/vnd.smartbear.drift.result \
    --verifier drift
fi
exit "$DRIFT_EXIT_CODE"
