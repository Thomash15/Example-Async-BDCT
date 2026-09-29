#!/usr/bin/env bash
set -euo pipefail

DEMO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

consumer() {
  (
    cd "$DEMO_ROOT/consumer-java-kafka"
    ./gradlew test --tests 'io.pactflow.example.kafka.ProductsPactTestV4' --rerun-tasks --console=plain
  )
  python3 - "$DEMO_ROOT" <<'PY'
import json
import sys
from pathlib import Path

folder = Path(sys.argv[1]) / "consumer-java-kafka/build/pacts"
matches = []
for path in folder.glob("*.json"):
    pact = json.loads(path.read_text())
    if pact["consumer"]["name"] == "pactflow-example-consumer-java-kafka-V4":
        matches.append(path)
        assert pact["interactions"], "No consumer interactions were generated"
        for interaction in pact["interactions"]:
            assert interaction["type"] == "Asynchronous/Messages"
            assert interaction["comments"]["references"]["AsyncAPI"]["operationId"] == "sendProductEvent"
assert len(matches) == 1, f"Expected one V4 consumer contract, found {matches}"
print("Consumer passed; AsyncAPI operation reference verified:", matches[0])
PY
}

provider() {
  (
    cd "$DEMO_ROOT/provider-java-kafka"
    BDCT_SCHEMA_FILE="$DEMO_ROOT/provider-java-kafka/asyncapi.json" \
      ./gradlew test --tests 'io.pactflow.example.kafka.ProductAsyncApiTest' --rerun-tasks --console=plain
  )
}

broken_provider() {
  python3 - "$DEMO_ROOT" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1]) / "provider-java-kafka"
document = json.loads((root / "asyncapi.json").read_text())
schema = document["components"]["schemas"]["ProductEvent"]
schema["properties"]["productName"] = schema["properties"].pop("name")
schema["required"] = ["productName" if field == "name" else field for field in schema["required"]]
for example in document["components"]["messages"]["ProductEvent"]["examples"]:
    payload = example["payload"]
    payload["productName"] = payload.pop("name")
target = root / "build/bdct/asyncapi-breaking.json"
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text(json.dumps(document, indent=2) + "\n")
print("Demo contract now requires productName:", target)
PY
  echo "Expected failure: the real producer still emits name. No source files were changed."
  (
    cd "$DEMO_ROOT/provider-java-kafka"
    BDCT_SCHEMA_FILE="$DEMO_ROOT/provider-java-kafka/build/bdct/asyncapi-breaking.json" \
      ./gradlew test --tests 'io.pactflow.example.kafka.ProductAsyncApiTest' --rerun-tasks --console=plain
  )
}

case "${1:-help}" in
  consumer) consumer ;;
  provider) provider ;;
  baseline) consumer; provider ;;
  broken-provider) broken_provider ;;
  *)
    echo "Usage: bash bdct-demo.sh {baseline|consumer|provider|broken-provider}"
    echo "baseline: run the consumer Pact test and independent provider schema tests"
    echo "broken-provider: deliberately fail provider validation against a changed schema"
    echo "provider: run against the original schema again (recovery)"
    echo "These are local checks. See provider-java-kafka/BDCT.md for PactFlow cross-contract comparison."
    ;;
esac
