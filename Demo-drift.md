# Runtime drift detection with Drift

This is an optional complementary demo that shows how a provider implementation can silently drift away from its AsyncAPI contract while the contract tests still look healthy.

The core idea: rename a field in the Java implementation while leaving the AsyncAPI specification unchanged. Drift detects that the emitted Kafka event no longer matches the declared schema.

## 1. Start Kafka and the provider

This sets up the runtime environment so the provider can emit real Kafka events.

From `provider-java-kafka`:

```bash
make drift-up
SEND_TEST_EVENTS=false ./gradlew bootRun
```

Wait for the application to start.

## 2. Show the passing baseline

This confirms the running provider emits events that match the AsyncAPI contract.

In another terminal, also in `provider-java-kafka`:

```bash
make drift-test
```

Expected result: `1 passed, 0 failed`.

Explanation: "Our running provider publishes an event that matches our AsyncAPI specification."

## 3. Introduce implementation drift

This intentionally breaks the contract at the Java implementation level while leaving the AsyncAPI schema unchanged.

In `provider-java-kafka/src/main/java/io/pactflow/example/kafka/ProductEvent.java`, uncomment the annotation above `name`:

```java
@com.fasterxml.jackson.annotation.JsonProperty("productName")
private String name;
```

Leave `provider-java-kafka/asyncapi.json` unchanged. Its schema still requires `name`.

## 4. Restart the provider

This ensures the running provider now uses the modified code with the field rename.

In the terminal running the provider, press `Ctrl+C`, then run:

```bash
SEND_TEST_EVENTS=false ./gradlew bootRun
```

## 5. Show Drift catching the mismatch

This demonstrates that Drift detects the runtime schema violation.

In the second terminal:

```bash
make drift-test
```

Expected result: failure, including a schema violation because the required `name` property is missing.

Explanation: "The implementation now emits `productName`, but the contract promises `name`. Drift captures the real Kafka event and rejects it."

## 6. Restore the provider and recover

This shows that fixing the implementation makes the drift check pass again.

Comment the annotation out again:

```java
// @com.fasterxml.jackson.annotation.JsonProperty("productName")
private String name;
```

Restart the provider and rerun:

```bash
make drift-test
```

Expected result: `1 passed, 0 failed`.

## 7. Clean up

Stop the provider with `Ctrl+C`, then:

```bash
make drift-down
```

## Key takeaway

You do not need PactFlow publication for this demo. Drift detects the mismatch at runtime and exits with a failure. A CI pipeline that requires this check to pass is what prevents the change from being deployed.

This optional Drift path complements the main BDCT flow. It shows that a provider can look healthy in isolation, but still fail when real runtime output drifts away from the declared contract.
