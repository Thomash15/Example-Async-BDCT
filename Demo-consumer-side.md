# Publish the consumer contract to PactFlow

This is the step-by-step demo flow for generating the consumer Pact contract and publishing it to PactFlow.

## 1. Set the environment variables

This prepares the connection to your PactFlow tenant and defines the version and participant names used in the demo. Without these values, the CLI cannot publish the contract to the correct workspace.

```bash
export PACT_BROKER_BASE_URL="https://YOUR-TENANT.pactflow.io"
export PACT_BROKER_TOKEN="YOUR_API_TOKEN"

export DEMO_VERSION="demo-$(date -u +%Y%m%dT%H%M%SZ)"
export CONSUMER="pactflow-example-consumer-java-kafka-V4"
export PROVIDER="pactflow-example-provider-java-kafka"

pact_cli() {
  docker run --rm \
    -v "$PWD:/work" -w /work \
    -e PACT_BROKER_BASE_URL \
    -e PACT_BROKER_TOKEN \
    pactfoundation/pact-cli:latest "$@"
}
```

## 2. Generate the consumer Pact contract

This runs the consumer test and creates the contract that represents the consumer's API expectations. The contract is the artifact we will publish to PactFlow for provider verification.

Run the consumer test script to generate the V4 contract:

```bash
bash bdct-demo.sh consumer
```

This validates that:
- the consumer test passes
- a Kafka async message contract was generated
- the AsyncAPI operation is `sendProductEvent`

The generated file should be:

```bash
consumer-java-kafka/build/pacts/${CONSUMER}-${PROVIDER}.json
```

## 3. Publish the contract to PactFlow

This uploads the generated contract to the PactFlow broker so the provider can be checked against the agreed consumer expectations.

```bash
pact_cli pact-broker publish \
  "consumer-java-kafka/build/pacts/${CONSUMER}-${PROVIDER}.json" \
  --consumer-app-version "$DEMO_VERSION" \
  --branch bdct-demo
```

This publishes the exact Pact file produced by the consumer test and tags it with the demo branch.

## 4. Verify the published version

This confirms the contract was accepted by PactFlow and that the version we published matches the consumer/provider relationship being demonstrated.

After publishing, open the PactFlow UI and confirm:
- consumer = `pactflow-example-consumer-java-kafka-V4`
- provider = `pactflow-example-provider-java-kafka`
- version = the `$DEMO_VERSION` value
- interaction metadata references the AsyncAPI operation `sendProductEvent`

## 5. Optional: confirm the generated Pact contents

This lets us inspect the actual JSON payload to ensure the contract includes the expected Kafka event metadata and message reference.

```bash
cat "consumer-java-kafka/build/pacts/${CONSUMER}-${PROVIDER}.json"
```

Look for the AsyncAPI reference:

```json
"comments": {
  "references": {
    "AsyncAPI": {
      "operationId": "sendProductEvent"
    }
  }
}
```

## Notes

- Use the generated Pact file, not the Java source code, as the published artifact.
- Keep the consumer version fixed for the rest of the demo so provider compatibility checks are clear.
- The contract is published successfully before compatibility is confirmed.

## Quick summary

This is the short version of the flow for a quick demo or handoff. It performs the same steps: generate the contract, publish it to PactFlow, and keep the demo version consistent.

```bash
export PACT_BROKER_BASE_URL="https://YOUR-TENANT.pactflow.io"
export PACT_BROKER_TOKEN="YOUR_API_TOKEN"
export DEMO_VERSION="demo-$(date -u +%Y%m%dT%H%M%SZ)"
export CONSUMER="pactflow-example-consumer-java-kafka-V4"
export PROVIDER="pactflow-example-provider-java-kafka"

bash bdct-demo.sh consumer

pact_cli() {
  docker run --rm \
    -v "$PWD:/work" -w /work \
    -e PACT_BROKER_BASE_URL \
    -e PACT_BROKER_TOKEN \
    pactfoundation/pact-cli:latest "$@"
}

pact_cli pact-broker publish \
  "consumer-java-kafka/build/pacts/${CONSUMER}-${PROVIDER}.json" \
  --consumer-app-version "$DEMO_VERSION" \
  --branch bdct-demo
```

## Next step

When the consumer contract has been published and verified, continue with the provider-side flow:

- [Provider-side demo](./Demo-provider-side.md)
- [Project README](./README.md)

This moves from the consumer contract generation and publication flow into the provider validation, deployment gate, and breaking-change demonstration.
