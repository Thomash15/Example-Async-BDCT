# Provider-side demo: publish, gate, break, and recover

This is the provider-side flow for the demo. It shows how to confirm a healthy baseline, publish the provider contract to PactFlow, create a dedicated demo environment, enforce the deployment gate, intentionally break compatibility, and then restore the provider to a passing state.

## 1. Start with the published, passing baseline

This establishes the known-good starting point. Before we introduce any change, we confirm that the current consumer and provider versions are compatible so the rest of the demo has a trustworthy baseline.

```bash
pact_cli pact-broker can-i-deploy \
  --pacticipant "$CONSUMER" --version "$DEMO_VERSION" \
  --pacticipant "$PROVIDER" --version "$DEMO_VERSION"
```

Expected result: successful compatibility check.

If the result is unknown or fails, resolve the issue before continuing. It is important that the baseline is genuinely green before we show the breaking change.

## 2. Create an isolated demonstration environment

This creates a dedicated demo environment so we can record simulated deployment state without affecting production or other environments. It provides a clean place to prove that the baseline is deployed and then see whether a candidate version is blocked.

```bash
export DEMO_ENV="customer-demo-$(date -u +%Y%m%dT%H%M%SZ)"

pact_cli pact-broker create-environment \
  --name "$DEMO_ENV" \
  --display-name "Customer demo" \
  --no-production

pact_cli pact-broker record-deployment \
  --pacticipant "$CONSUMER" \
  --version "$DEMO_VERSION" \
  --environment "$DEMO_ENV"

pact_cli pact-broker record-deployment \
  --pacticipant "$PROVIDER" \
  --version "$DEMO_VERSION" \
  --environment "$DEMO_ENV"
```

## 3. Define the deployment gate

This is the key control point. The deployment gate checks whether a candidate provider version is compatible with the already-deployed consumer version before the deployment step is allowed to proceed.

```bash
deploy_demo() {
  if pact_cli pact-broker can-i-deploy \
    --pacticipant "$PROVIDER" \
    --version "$CANDIDATE_VERSION" \
    --to-environment "$DEMO_ENV" \
    --retry-while-unknown 12 \
    --retry-interval 5; then
    echo "DEPLOYMENT ALLOWED — simulated deployment step"
  else
    echo "DEPLOYMENT BLOCKED — deployment step was not executed"
    return 1
  fi
}

export CANDIDATE_VERSION="$DEMO_VERSION"
deploy_demo
```

This should pass for the baseline provider. It proves the gate allows a compatible change to continue.

## 4. Introduce a breaking provider change

This is where we show the risk of a contract change that looks valid internally but breaks the existing consumer contract. We keep the consumer unchanged and only change the provider-side schema and implementation contract.

Update the Java model and the AsyncAPI contract in the exact areas below:

- In `provider-java-kafka/src/main/java/io/pactflow/example/kafka/ProductEvent.java`, change the field around line 12-13, where `name` is currently declared. Add the annotation shown below so the JSON field is renamed to `productName` while keeping the Java field name as `name`:

```java
@com.fasterxml.jackson.annotation.JsonProperty("productName")
private String name;
```

- In `provider-java-kafka/asyncapi.json`, edit the `ProductEvent` schema block under `components.schemas.ProductEvent` and the example payload under `components.messages.ProductEvent.examples[0].payload`.
  - change `properties.name` to `properties.productName`
  - change the required array from `["id", "name", "type", "version", "event"]` to `["id", "productName", "type", "version", "event"]`
  - update the example payload key from `"name": "product name"` to `"productName": "product name"`

This intentionally changes the contract shape while leaving the consumer version unchanged.

## 5. Run the provider tests

This verifies the provider's own implementation against the changed schema. The provider may still pass its internal tests, which is exactly the point of the demonstration: self-verification alone is not enough.

```bash
export CANDIDATE_VERSION="${DEMO_VERSION}-breaking"

if bash bdct-demo.sh provider \
  > provider-java-kafka/build/bdct/provider-test.log 2>&1; then
  PROVIDER_TEST_EXIT=0
else
  PROVIDER_TEST_EXIT=$?
fi

cat provider-java-kafka/build/bdct/provider-test.log
```

Expected result: all four provider tests pass.

## 6. Publish the new provider version

This publishes the changed provider contract and the actual provider test result to PactFlow. We keep the consumer version unchanged so PactFlow can compare the old consumer contract with the new provider contract.

```bash
pact_cli pactflow publish-provider-contract \
  provider-java-kafka/asyncapi.json \
  --provider "$PROVIDER" \
  --provider-app-version "$CANDIDATE_VERSION" \
  --branch bdct-demo \
  --specification asyncapi \
  --content-type application/json \
  --verification-exit-code "$PROVIDER_TEST_EXIT" \
  --verification-results provider-java-kafka/build/bdct/provider-test.log \
  --verification-results-content-type text/plain \
  --verifier "JUnit5-networknt-json-schema"
```

This publishes the new provider version without changing the consumer version. The comparison is now between the current consumer expectations and the changed provider contract.

## 7. Attempt deployment through the gate

This is the proof that the deployment gate stops a provider version that would be incompatible with the already deployed consumer. The result is nonzero, so the deployment step is never executed.

```bash
deploy_demo
```

Expected result:

```text
DEPLOYMENT BLOCKED — deployment step was not executed
```

In Swagger Contract Testing, show:
- Provider self-verification: pass
- Consumer Pact versus new AsyncAPI: fail
- Deployment gate: blocked

This demonstrates exactly why `can-i-deploy` matters: it prevents a deployment from progressing even when the provider appears internally correct.

## 8. Show recovery and restore compatibility

This step returns the provider to the original contract shape and proves the gate allows the restored version. It highlights the full lifecycle: break, detect, fix, and re-verify.

Restore the contract so that the field is `name` again in both the Java implementation and the AsyncAPI schema. Then run the same provider test and publish flow for the restored provider version.

```bash
export CANDIDATE_VERSION="${DEMO_VERSION}-restored"

if bash bdct-demo.sh provider \
  > provider-java-kafka/build/bdct/provider-test.log 2>&1; then
  PROVIDER_TEST_EXIT=0
else
  PROVIDER_TEST_EXIT=$?
fi

cat provider-java-kafka/build/bdct/provider-test.log

pact_cli pactflow publish-provider-contract \
  provider-java-kafka/asyncapi.json \
  --provider "$PROVIDER" \
  --provider-app-version "$CANDIDATE_VERSION" \
  --branch bdct-demo \
  --specification asyncapi \
  --content-type application/json \
  --verification-exit-code "$PROVIDER_TEST_EXIT" \
  --verification-results provider-java-kafka/build/bdct/provider-test.log \
  --verification-results-content-type text/plain \
  --verifier "JUnit5-networknt-json-schema"

export CANDIDATE_VERSION="$DEMO_VERSION-restored"
deploy_demo
```

Expected result: provider tests pass, compatibility passes, and the deployment gate allows the restored version.

## Optional: run the Drift demo

This is a complementary runtime drift check. It is not required for the main PactFlow BDCT flow, but it is useful when you want to show how a provider implementation can silently drift away from the AsyncAPI contract while the contract tests still look healthy.

### 1. Start Kafka and the provider

From `provider-java-kafka`:

```bash
make drift-up
SEND_TEST_EVENTS=false ./gradlew bootRun
```

Wait for the application to start.

### 2. Show the passing baseline

In another terminal, also in `provider-java-kafka`:

```bash
make drift-test
```

Expected result: `1 passed, 0 failed`.

This confirms the running provider emits an event that matches the AsyncAPI contract.

### 3. Introduce implementation drift

In `provider-java-kafka/src/main/java/io/pactflow/example/kafka/ProductEvent.java`, uncomment the annotation above `name`:

```java
@com.fasterxml.jackson.annotation.JsonProperty("productName")
private String name;
```

Leave `provider-java-kafka/asyncapi.json` unchanged. Its schema still requires `name`.

### 4. Restart the provider

In the terminal running the provider, stop it with `Ctrl+C`, then run:

```bash
SEND_TEST_EVENTS=false ./gradlew bootRun
```

### 5. Show Drift catching the mismatch

In the second terminal:

```bash
make drift-test
```

Expected result: failure, including a schema violation because the required `name` property is missing.

This demonstrates the runtime problem: the provider emits `productName`, but the contract still promises `name`.

### 6. Restore the provider and recover

Comment the annotation out again:

```java
// @com.fasterxml.jackson.annotation.JsonProperty("productName")
private String name;
```

Restart the provider and rerun:

```bash
make drift-test
```

Expected result: the check passes again.

### 7. Clean up

Stop the provider with `Ctrl+C`, then:

```bash
make drift-down
```

This optional Drift path complements the main BDCT flow. It shows that a provider can look healthy in isolation, but still fail when real runtime output drifts away from the declared contract.

---

## Summary

This demo shows the full provider-side story:
- confirm a working baseline
- record the deployment environment state
- enforce a deployment gate
- introduce a breaking contract change
- prove provider self-verification alone is not enough
- block the incompatible deployment
- restore compatibility and pass the gate again

It is not enough for a provider to pass its own tests; it must also remain compatible with the contract the consumer is actually using.
